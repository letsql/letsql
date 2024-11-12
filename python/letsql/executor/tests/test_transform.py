import ibis
import pandas as pd

import letsql as ls
from letsql.common.caching import SourceStorage
from letsql.executor.segment import segment
from letsql.executor.transform import transform_plan
from letsql.expr.relations import into_backend
from letsql.tests.util import assert_frame_equal


def test_simple_expr(pg):
    t = pg.table("batting")

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


def test_simple_cached(pg):
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
    expr = node.to_expr()
    actual = expr.execute()

    pg_t = pg.table("batting")
    pg_expr = (
        pg_t.join(pg_t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    expected = pg_expr.execute()

    assert_frame_equal(actual, expected)


def test_transform_into_backend(pg):
    con = ibis.duckdb.connect()
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

    pg_t = pg.table("batting")
    pg_expr = (
        pg_t.join(pg_t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    expected = pg_expr.execute()

    assert_frame_equal(actual, expected)


def test_segment_into_backend_cache(pg):
    con = ls.connect()
    ddb_con = ibis.duckdb.connect()

    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
        .cache(SourceStorage(source=ddb_con))
    )

    plan = segment(expr.op())
    node = transform_plan(plan)
    actual = node.to_expr().execute()

    pg_t = pg.table("batting")
    pg_expr = (
        pg_t.join(pg_t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    expected = pg_expr.execute()

    assert_frame_equal(actual, expected)


def test_multiple_into_backend_cache(pg):
    con = ls.connect()
    ddb_con = ibis.duckdb.connect()

    t = into_backend(pg.table("batting"), con, "ls_batting")
    tj = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID")
        .limit(15)
        .select(player_id="playerID", year_id="yearID")
    )

    ddb_tj = into_backend(tj, ddb_con)
    expr = (
        ddb_tj.join(ddb_tj, "player_id")
        .order_by("player_id", "year_id")
        .select("player_id", "year_id")
        .limit(15)
    )

    plan = segment(expr.op())
    node = transform_plan(plan)
    actual = node.to_expr().execute()

    pg_t = pg.table("batting")
    pg_expr = (
        pg_t.join(pg_t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    pg_expr = (
        pg_expr.join(pg_expr, "player_id")
        .order_by("player_id", "year_id")
        .select("player_id", "year_id")
        .limit(15)
    )

    expected = pg_expr.execute()
    assert_frame_equal(actual, expected)
