import pytest
import pyarrow as pa

import ibis.expr.datatypes as dt
from ibis import _
from ibis import literal as L

from letsql.expr import udf
from letsql.tests.util import assert_frame_equal

pc = pytest.importorskip("pyarrow.compute")


@udf.agg.pyarrow
def my_mean(arr: dt.float64) -> dt.float64:
    return pc.mean(arr)


@udf.agg.pyarrow
def add_mean(a: dt.float64, b: dt.float64) -> dt.float64:
    return pc.mean(pc.add(a, b))


return_type = pa.struct(
    [
        ("a", pa.float64()),
        ("b", pa.float64()),
        ("c", pa.float64()),
    ]
)


@udf.agg.pyarrow
def centroid(a: dt.float64, b: dt.float64, c: dt.float64) -> return_type:
    return pa.scalar((pc.mean(a), pc.mean(b), pc.mean(c)), type=return_type)


@udf.agg.pyarrow
def centroid_list(a: dt.float64, b: dt.float64, c: dt.float64) -> pa.list_(
    pa.float64()
):
    return pa.scalar([pc.mean(a), pc.mean(b), pc.mean(c)], type=pa.list_(pa.float64()))


def test_udf_agg_pyarrow(con, batting):
    batting = con.register(batting.execute(), "pg-batting")
    result = my_mean(batting.G).execute()

    assert result == batting.G.execute().mean()


def test_multiple_arguments_udf_agg_pyarrow(con, batting):
    batting = con.register(batting.execute(), "pg-batting")
    actual = add_mean(batting.G, batting.G).execute()
    expected = batting.G.execute()
    expected = (expected + expected).mean()

    assert actual == expected


def test_multiple_arguments_struct_udf_agg_pyarrow(con, batting):
    from math import isclose

    batting = con.register(batting.execute(), "pg-batting")
    actual = centroid(batting.G, batting.G, batting.G).execute()
    expected = batting.G.execute().mean()

    assert all(isclose(value, expected) for value in actual.values())


def test_multiple_arguments_list_udf_agg_pyarrow(con, batting):
    from math import isclose

    batting = con.register(batting.execute(), "pg-batting")
    actual = centroid_list(batting.G, batting.G, batting.G).execute()
    expected = batting.G.execute().mean()

    assert all(isclose(value, expected) for value in actual)


@udf.agg.pyarrow
def my_sum(arr: dt.float64) -> dt.float64:
    return pc.sum(arr)


def test_group_by_udf_agg_pyarrow(con, alltypes_df):
    alltypes = con.register(alltypes_df, "pg-alltypes")

    expr = (
        alltypes[_.string_col == "1"]
        .mutate(x=L(1, "int64"))
        .group_by(_.x)
        .aggregate(sum=my_sum(_.double_col))
    )

    result = expr.execute().astype({"x": "int64"})
    expected = (
        alltypes_df.loc[alltypes_df.string_col == "1", :]
        .assign(x=1)
        .groupby("x")
        .double_col.sum()
        .rename("sum")
        .reset_index()
    )

    assert_frame_equal(result, expected, check_like=True)
