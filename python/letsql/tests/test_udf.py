from __future__ import annotations

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pandas.testing as tm
import pyarrow as pa
import pytest
from ibis import udf

import letsql as ls

pc = pytest.importorskip("pyarrow.compute")


@ibis.udf.scalar.pyarrow
def my_string_length(arr: dt.string) -> dt.int64:
    # arr is a pyarrow.StringArray
    return pc.cast(pc.multiply(pc.utf8_length(arr), 2), target_type="int64")


@ibis.udf.scalar.pyarrow
def my_add(arr1: dt.int64, arr2: dt.int64) -> dt.int64:
    return pc.add(arr1, arr2)


@ibis.udf.scalar.pyarrow
def small_add(arr1: dt.int32, arr2: dt.int32) -> dt.int64:
    return pc.cast(pc.add(arr1, arr2), pa.int64())


@ibis.udf.agg.builtin
def my_mean(arr: dt.float64) -> dt.float64:
    return pc.mean(arr)


def test_udf(alltypes):
    data_string_col = ls.execute(alltypes.date_string_col)
    expected = data_string_col.str.len() * 2

    expr = my_string_length(alltypes.date_string_col)
    assert isinstance(expr, ir.Column)

    result = ls.execute(expr)
    tm.assert_series_equal(result, expected, check_names=False)


def test_multiple_argument_udf(alltypes):
    expr = small_add(alltypes.smallint_col, alltypes.int_col).name("tmp")
    result = ls.execute(expr)

    df = alltypes[["smallint_col", "int_col"]].execute()
    expected = (df.smallint_col + df.int_col).astype("int64")

    tm.assert_series_equal(result, expected.rename("tmp"))


def test_builtin_scalar_udf(con):
    @udf.scalar.builtin
    def to_hex(a: int) -> str:
        """Convert an integer to a hex string."""

    expr = to_hex(42)
    result = con.execute(expr)
    assert result == "2a"


def test_builtin_agg_udf(con):
    @udf.agg.builtin
    def median(a: float) -> float:
        """Median of a column."""

    expr = median(con.tables.batting.G)
    result = con.execute(expr)
    assert result == ls.execute(con.tables.batting.G).median()


def test_builtin_agg_udf_filtered(con):
    @udf.agg.builtin
    def median(a: float, where: bool = True) -> float:
        """Median of a column."""

    ls.execute(median(con.tables.batting.G))
