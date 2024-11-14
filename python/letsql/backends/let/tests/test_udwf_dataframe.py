import ibis
import ibis.expr.datatypes as dt
import numpy as np
import pyarrow as pa
import pytest

import letsql
from letsql.expr import udf

from ibis import _


@pytest.fixture
def df():
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([0, 1, 2, 3, 4, 5, 6]),
            pa.array([7, 4, 3, 8, 9, 1, 6]),
            pa.array(["A", "A", "A", "A", "B", "B", "B"]),
        ],
        names=["a", "b", "c"],
    )

    return batch.to_pandas()


@udf.window.pyarrow(evaluation="all")
def smooth_default(values: dt.float64) -> dt.float64:
    results = []
    curr_value = None
    for value in values:
        if curr_value is None:
            curr_value = value.as_py()
        else:
            curr_value = value.as_py() * 0.9 + curr_value * 0.1
        results.append(curr_value)
    return pa.array(results)


def get_range(idx):
    if idx == 0:
        return 0, 0
    return idx - 1, idx


@udf.window.pyarrow(evaluation="one", range=get_range)
def smooth_bounded(values: dt.float64) -> dt.float64:
    curr_value = None
    for value in values:
        value = value.as_py()
        if curr_value is None:
            curr_value = value
        else:
            curr_value = value * 0.9 + curr_value * 0.1

    return pa.scalar(curr_value).cast(pa.float64())


@pytest.mark.parametrize(
    "window,expected",
    [
        (ibis.window(), [0, 0.9, 1.89, 2.889, 3.889, 4.889, 5.889]),
        (ibis.window(group_by=_.c), [0, 0.9, 1.89, 2.889, 4.0, 4.9, 5.89]),
        (ibis.window(order_by=_.b), [0.551, 1.13, 2.3, 2.755, 3.876, 5.0, 5.513]),
    ],
)
def test_smooth_default(df, window, expected):
    con = letsql.connect()
    t = con.register(df, table_name="t")

    expr = t.select(
        t.a,
        udwf=smooth_default(t.a).over(window),
    ).order_by(t.a)

    result = expr.execute()
    actual = result["udwf"].to_list()

    np.testing.assert_allclose(actual, expected, rtol=1e-3)


@pytest.mark.parametrize(
    "window,expected",
    [
        (ibis.window(), [0, 0.9, 1.9, 2.9, 3.9, 4.9, 5.9]),
    ],
)
def test_smooth_bounded(df, window, expected):
    con = letsql.connect()
    t = con.register(df, table_name="t")

    expr = t.select(
        t.a,
        udwf=smooth_bounded(t.a).over(window),
    ).order_by(t.a)

    result = expr.execute()
    actual = result["udwf"].to_list()

    np.testing.assert_allclose(actual, expected, rtol=1e-3)
