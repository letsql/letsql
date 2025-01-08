from __future__ import annotations

from typing import Callable

import ibis.expr.types as ir
from ibis import memtable
import pytest
from pytest import param

import letsql as ls
from letsql.tests.conftest import TEST_TABLES
from letsql.tests.util import assert_frame_equal


def test_list_tables(con):
    tables = con.list_tables()
    assert isinstance(tables, list)
    key = "functional_alltypes"
    assert key in tables or key.upper() in tables
    assert all(isinstance(table, str) for table in tables)


def test_tables_accessor_mapping(con):
    if con.name == "snowflake":
        pytest.skip("snowflake sometimes counts more tables than are around")

    name = "functional_alltypes"

    assert isinstance(con.tables[name], ir.Table)

    with pytest.raises(KeyError, match="doesnt_exist"):
        con.tables["doesnt_exist"]

    # temporary might pop into existence in parallel test runs, in between the
    # first `list_tables` call and the second, so we check that there's a
    # non-empty intersection
    assert TEST_TABLES.keys() & set(map(str.lower, con.list_tables()))
    assert TEST_TABLES.keys() & set(map(str.lower, con.tables))


def test_tables_accessor_getattr(con):
    name = "functional_alltypes"
    assert isinstance(getattr(con.tables, name), ir.Table)

    with pytest.raises(AttributeError, match="doesnt_exist"):
        con.tables.doesnt_exist  # noqa: B018

    # Underscore/double-underscore attributes are never available, since many
    # python apis expect that checking for the absence of these to be cheap.
    with pytest.raises(AttributeError, match="_private_attr"):
        con.tables._private_attr  # noqa: B018


def test_tables_accessor_tab_completion(con):
    name = "functional_alltypes"
    attrs = dir(con.tables)
    assert name in attrs
    assert "keys" in attrs  # type methods also present

    keys = con.tables._ipython_key_completions_()
    assert name in keys


def test_tables_accessor_repr(con):
    name = "functional_alltypes"
    result = repr(con.tables)
    assert f"- {name}" in result


@pytest.mark.parametrize(
    "expr_fn",
    [
        param(lambda t: t.limit(5).limit(10), id="small_big"),
        param(lambda t: t.limit(10).limit(5), id="big_small"),
    ],
)
def test_limit_chain(alltypes, expr_fn):
    expr = expr_fn(alltypes)
    result = ls.execute(expr)
    assert len(result) == 5


@pytest.mark.parametrize(
    "expr_fn",
    [
        param(lambda t: t, id="alltypes table"),
        param(lambda t: t.join(t.view(), [("id", "int_col")]), id="self join"),
    ],
)
def test_unbind(alltypes, expr_fn: Callable):
    ls.options.interactive = False

    expr = expr_fn(alltypes)
    assert expr.unbind() != expr
    assert expr.unbind().schema() == expr.schema()

    assert "Unbound" not in repr(expr)
    assert "Unbound" in repr(expr.unbind())


@pytest.mark.parametrize(
    ("extension", "method"),
    [("parquet", ls.read_parquet), ("csv", ls.read_csv)],
)
def test_read(data_dir, extension, method):
    table = method(
        data_dir / extension / f"batting.{extension}", table_name=f"batting-{extension}"
    )
    assert ls.execute(table) is not None


def test_train_test_split():
    # Check counts and overlaps in train and test dataset
    N = 100
    test_size = 0.25
    # init table
    table = memtable([(i, "val") for i in range(N)], columns=["key1", "val"])
    train_table, test_table = ls.train_test_split(
        table, unique_key="key1", test_size=test_size, random_seed=42
    )
    # These values are for seed 42
    assert train_table.count().execute() == 73
    assert test_table.count().execute() == 27
    assert set(train_table.columns) == set(table.columns)
    assert set(test_table.columns) == set(table.columns)
    # make sure data unioned together is itself
    assert train_table.union(test_table).join(table, how="semi").count().execute() == N

    # Check reproducibility
    reproduced_train_table, reproduced_test_table = ls.train_test_split(
        table, unique_key="key1", test_size=test_size, random_seed=42
    )
    assert_frame_equal(train_table.execute(), reproduced_train_table.execute())
    assert_frame_equal(test_table.execute(), reproduced_test_table.execute())

    # make sure it could generate different data with different random_seed
    different_train_table, different_test_table = ls.train_test_split(
        table, unique_key="key1", test_size=test_size, random_seed=0
    )
    assert not train_table.execute().equals(different_train_table.execute())
    assert not test_table.execute().equals(different_test_table.execute())


def test_train_test_split_invalid_test_size():
    table = memtable({"key": [1, 2, 3]})
    with pytest.raises(ValueError, match="test size should be a float between 0 and 1"):
        ls.train_test_split(table, unique_key="key", test_size=1.5)
    with pytest.raises(ValueError, match="test size should be a float between 0 and 1"):
        ls.train_test_split(table, unique_key="key", test_size=-0.5)


def test_train_test_split_invalid_num_buckets_type():
    table = memtable({"key": [1, 2, 3]})
    with pytest.raises(ValueError, match="num_buckets must be an integer"):
        ls.train_test_split(table, unique_key="key", test_size=0.5, num_buckets=10.5)


def test_train_test_split_invalid_num_buckets_value():
    table = memtable({"key": [1, 2, 3]})
    with pytest.raises(
        ValueError, match="num_buckets = 1 places all data into training set"
    ):
        ls.train_test_split(table, unique_key="key", test_size=0.5, num_buckets=1)


def test_train_test_split_multiple_keys():
    data = {
        "key1": range(100),
        "key2": [chr(i % 26 + 65) for i in range(100)],  # A, B, C, ...
        "value": [i % 3 for i in range(100)],
    }
    table = memtable(data)
    train_table, test_table = ls.train_test_split(
        table,
        unique_key=["key1", "key2"],
        test_size=0.25,
        num_buckets=10,
        random_seed=99,
    )
    assert train_table.union(test_table).join(table, how="anti").count().execute() == 0
