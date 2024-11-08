from letsql.common.caching import SourceStorage
from letsql.executor.segment import segment
from letsql.executor.transform import transform_plan

import pandas as pd


def test_transform_plan(pg):
    t = pg.table("batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
        .cache(SourceStorage(pg))
    )

    plan = segment(expr.op())
    node = transform_plan(plan)

    result = node.to_expr().execute()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 15
