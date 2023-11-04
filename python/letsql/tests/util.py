from typing import Any

import pandas as pd
import pandas.testing as tm


reduction_tolerance = 1e-7


def assert_frame_equal(
    left: pd.DataFrame, right: pd.DataFrame, *args: Any, **kwargs: Any
) -> None:
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)
    kwargs.setdefault("check_dtype", True)
    tm.assert_frame_equal(left, right, *args, **kwargs)
