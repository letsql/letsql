from typing import Any

import pandas as pd
import pandas.testing as tm


def assert_frame_equal(left: pd.DataFrame, right: pd.DataFrame, *args: Any) -> None:
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)
    tm.assert_frame_equal(left, right, *args, check_dtype=True)
