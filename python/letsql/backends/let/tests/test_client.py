import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest import param

import letsql as ls
from letsql.tests.util import (
    assert_frame_equal,
)


def test_register_record_batch_reader(alltypes_df):
    con = ls.connect()
    t = con.register(alltypes_df, "alltypes")
    record_batch_reader = t.to_pyarrow_batches()
    t2 = con.register(record_batch_reader, "t2")
    actual = ls.execute(t2)

    assert isinstance(record_batch_reader, pa.RecordBatchReader)
    assert_frame_equal(actual, alltypes_df)


def test_register_expr(alltypes_df):
    con = ls.connect()
    t = con.register(alltypes_df, "alltypes")
    t2 = con.register(t, "alltypes2")
    actual = ls.execute(t2)
    assert_frame_equal(actual, alltypes_df)


def test_execute_nonnull():
    schema = pa.schema((pa.field("x", pa.int64(), nullable=False),))
    tab = pa.table({"x": [1, 2, 3]}, schema=schema)
    con = ls.connect()
    t = con.register(tab, "my_table")
    ls.execute(t)


def test_register_record_batch_reader_with_filter(alltypes, alltypes_df):
    con = ls.connect()
    t = con.register(alltypes_df, "alltypes")
    record_batch_reader = t.to_pyarrow_batches()
    t2 = con.register(record_batch_reader, "t2")
    cols_a = [ca for ca in alltypes.columns.copy() if ca != "timestamp_col"]
    expr = t2.filter((t2.id >= 5200) & (t2.id <= 5210))[cols_a]
    ls.execute(expr)


def test_create_table(con):
    import pandas as pd

    con.create_table("UPPERCASE", schema=ibis.schema({"A": "int"}))
    con.create_table("name", pd.DataFrame({"a": [1]}))


def test_register_table_with_uppercase(ls_con):
    db_con = ls.duckdb.connect()
    db_t = db_con.create_table("lowercase", schema=ibis.schema({"A": "int"}))

    uppercase_table_name = "UPPERCASE"
    t = ls_con.register(db_t, uppercase_table_name)
    assert uppercase_table_name in ls_con.list_tables()
    assert ls.execute(t) is not None


def test_register_table_with_uppercase_multiple_times(ls_con):
    db_con = ls.duckdb.connect()
    db_t = db_con.create_table("lowercase", schema=ibis.schema({"A": "int"}))

    uppercase_table_name = "UPPERCASE"
    ls_con.register(db_t, uppercase_table_name)

    expected_schema = ibis.schema({"B": "int"})
    db_t = db_con.create_table("lowercase_2", schema=expected_schema)
    t = ls_con.register(db_t, uppercase_table_name)

    assert uppercase_table_name in ls_con.list_tables()
    assert ls.execute(t) is not None
    assert t.schema() == expected_schema


@pytest.mark.xfail(reason="datafusion metadata reading not working")
def test_parquet_expr_metadata_available(
    ls_con, parquet_metadata, parquet_path_with_metadata
):
    table_name = "t"
    ls_con.read_parquet(parquet_path_with_metadata, table_name=table_name)
    con_metadata = ls_con.con.table(table_name).schema().metadata
    assert not set(parquet_metadata.items()).difference(set(con_metadata.items()))


@pytest.mark.parametrize(
    "path",
    (
        param(
            "parquet_path_with_metadata",
            id="pathlib",
            marks=[],
        ),
        param(
            "s3://letsql-pytest/with-metadata.parquet",
            id="s3",
            marks=[pytest.mark.slow],
        ),
        param(
            "https://letsql-pytest.s3.us-east-2.amazonaws.com/with-metadata.parquet",
            id="https",
            marks=[
                pytest.mark.xfail(
                    reason="pyarrow.parquet.read_metadata can't do http/https"
                )
            ],
        ),
    ),
)
def test_parquet_metadata_readable(request, parquet_metadata, path):
    try:
        path = request.getfixturevalue(path)
    except Exception:
        pass
    metadata = pq.read_metadata(path)
    assert not set(parquet_metadata.items()).difference(set(metadata.metadata.items()))


def test_file_sort_order_injected(ls_con, parquet_path_with_metadata):
    table_name = "t"
    t = ls_con.read_parquet(parquet_path_with_metadata, table_name=table_name)
    expr = t.group_by("b").agg(max_a=t["a"].max())
    sql = f"EXPLAIN {ls.to_sql(expr)}"
    physical_plan = (
        ls_con.con.sql(sql)
        .to_pandas()
        .set_index("plan_type")
        .loc["physical_plan", "plan"]
    )
    assert "ordering_mode=Sorted" in physical_plan
