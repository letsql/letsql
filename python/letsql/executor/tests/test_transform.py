from letsql.common.caching import SourceStorage
from letsql.executor.segment import segment
from letsql.executor.transform import transform_plan

import pandas as pd

import letsql as ls
from letsql.expr.relations import into_backend


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


def test_transform_into_backend_join(pg):
    con = ls.connect()
    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
    )

    plan = segment(expr.op())
    node = transform_plan(plan)

    actual = node.to_expr().execute()
    assert isinstance(actual, pd.DataFrame)
    assert len(actual) == 15
