import ibis
import ibis.expr.datatypes as dt
import numpy as np
import pytest
from ibis import _

import letsql
from letsql.expr import udf
from letsql.backends.let.tests.common import (
    ExponentialSmoothDefault,
    ExponentialSmoothBounded,
    ExponentialSmoothFrame,
    ExponentialSmoothRank,
    SmoothTwoColumn,
)

smooth_bounded = udf.window.pyarrow(
    ExponentialSmoothBounded(0.9),
    input_types=[dt.float64],
    return_type=dt.float64,
    name="smooth_bounded",
)

smooth_two_column = udf.window.pyarrow(
    SmoothTwoColumn(0.9),
    input_types=[dt.int64, dt.int64],
    return_type=dt.float64,
    name="smooth_two_column",
)

smooth_rank = udf.window.pyarrow(
    ExponentialSmoothRank(0.9),
    input_types=dt.str,
    return_type=dt.float64,
    name="smooth_rank",
)

smooth_frame = udf.window.pyarrow(
    ExponentialSmoothFrame(0.9),
    input_types=[dt.float64],
    return_type=dt.float64,
    name="smooth_frame",
)

smooth_default = udf.window.pyarrow(
    ExponentialSmoothDefault(0.9),
    input_types=[dt.float64],
    return_type=dt.float64,
    name="smooth_default",
)


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


def test_smooth_bounded(df):
    con = letsql.connect()
    t = con.register(df, table_name="t")

    expr = t.select(
        t.a,
        udwf=smooth_bounded(t.a).over(ibis.window()),
    ).order_by(t.a)

    result = expr.execute()
    actual = result["udwf"].to_list()

    np.testing.assert_allclose(actual, [0, 0.9, 1.9, 2.9, 3.9, 4.9, 5.9], rtol=1e-3)


def test_smooth_two_column(df):
    con = letsql.connect()
    t = con.register(df, table_name="t")

    expr = t.select(
        t.a,
        udwf=smooth_two_column(t.a, t.b).over(ibis.window()),
    ).order_by(t.a)

    result = expr.execute()
    actual = result["udwf"].to_list()

    np.testing.assert_allclose(actual, [0.0, 1.0, 2.0, 2.2, 3.2, 5.0, 6.0], rtol=1e-3)


def test_smooth_rank(df):
    con = letsql.connect()
    t = con.register(df, table_name="t")

    expr = t.select(
        t.a,
        udwf=smooth_rank(t.c).over(ibis.window(order_by=t.c)),
    ).order_by(t.a)

    result = expr.execute()
    actual = result["udwf"].to_list()

    np.testing.assert_allclose(actual, [1, 1, 1, 1, 1.9, 2, 2], rtol=1e-3)


@pytest.mark.parametrize(
    "window,expected",
    [
        (ibis.window(), [5.889, 5.889, 5.889, 5.889, 5.889, 5.889, 5.889]),
        (ibis.cumulative_window(), [0.0, 0.9, 1.89, 2.889, 3.889, 4.889, 5.889]),
        (
            ibis.cumulative_window(order_by=_.b),
            [0.551, 1.13, 2.3, 2.755, 3.876, 5.0, 5.513],
        ),
    ],
)
def test_smooth_frame_bounded(df, window, expected):
    con = letsql.connect()
    t = con.register(df, table_name="t")

    expr = t.select(
        t.a,
        udwf=smooth_frame(t.a).over(window),
    ).order_by(t.a)

    result = expr.execute()
    actual = result["udwf"].to_list()

    np.testing.assert_allclose(actual, expected, rtol=1e-3)
