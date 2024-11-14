import ibis
import numpy as np
import pyarrow as pa
import pytest
from ibis import _

import letsql
from letsql.expr import udf
import ibis.expr.datatypes as dt

from letsql.internal import WindowEvaluator


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


class ExponentialSmoothDefault(WindowEvaluator):
    def __init__(self, alpha: float) -> None:
        self.alpha = alpha

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        results = []
        curr_value = 0.0
        values = values[0]
        for idx in range(num_rows):
            if idx == 0:
                curr_value = values[idx].as_py()
            else:
                curr_value = values[idx].as_py() * self.alpha + curr_value * (
                    1.0 - self.alpha
                )
            results.append(curr_value)

        return pa.array(results)


class ExponentialSmoothBounded(WindowEvaluator):
    def __init__(self, alpha: float) -> None:
        self.alpha = alpha

    def supports_bounded_execution(self) -> bool:
        return True

    def get_range(self, idx: int, num_rows: int) -> tuple[int, int]:
        # Override the default range of current row since uses_window_frame is False
        # So for the purpose of this test we just smooth from the previous row to
        # current.
        if idx == 0:
            return 0, 0
        return idx - 1, idx

    def evaluate(
        self, values: list[pa.Array], eval_range: tuple[int, int]
    ) -> pa.Scalar:
        (start, stop) = eval_range
        curr_value = 0.0
        values = values[0]
        for idx in range(start, stop + 1):
            if idx == start:
                curr_value = values[idx].as_py()
            else:
                curr_value = values[idx].as_py() * self.alpha + curr_value * (
                    1.0 - self.alpha
                )
        return pa.scalar(curr_value).cast(pa.float64())


smooth_default = udf.window.pyarrow(
    ExponentialSmoothDefault(0.9),
    signature=((dt.float64,), dt.float64),
    name="smooth_default",
)
smooth_bounded = udf.window.pyarrow(
    ExponentialSmoothBounded(0.9),
    signature=((dt.float64,), dt.float64),
    name="smooth_bounded",
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
