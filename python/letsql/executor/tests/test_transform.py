import ibis
import pandas as pd

import letsql as ls
from letsql.common.caching import SourceStorage, ParquetCacheStorage
from letsql.executor.segment import segment
from letsql.executor.transform import transform_plan
from letsql.expr.relations import into_backend
from letsql.tests.util import assert_frame_equal

from ibis import _

import pytest


@pytest.fixture(scope="session")
def trino_table():
    return (
        ibis.trino.connect(database="tpch", schema="sf1")
        .table("orders")
        .cast({"orderdate": "date"})
    )


def make_merged(expr):
    agg = expr.group_by(["custkey", "orderdate"]).agg(
        _.totalprice.sum().name("totalprice")
    )
    w = ibis.window(group_by="custkey", order_by="orderdate")
    windowed = (
        agg.mutate(_.totalprice.cumsum().over(w).name("curev"))
        .mutate(_.curev.lag(1).over(w).name("curev@t-1"))
        .select(["custkey", "orderdate", "curev", "curev@t-1"])
    )
    merged = expr.asof_join(
        windowed,
        on="orderdate",
        predicates="custkey",
    ).select(
        [expr[c] for c in expr.columns]
        + [windowed[c] for c in windowed.columns if c not in expr.columns]
    )
    return merged


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


def test_into_backend_duckdb_trino(trino_table):
    db_con = ibis.duckdb.connect()
    expr = trino_table.head(10_000).pipe(into_backend, db_con).pipe(make_merged)

    plan = segment(expr.op())
    node = transform_plan(plan)
    expr = node.to_expr()

    actual = expr.execute()

    assert isinstance(actual, pd.DataFrame)
    assert len(actual) > 0


def test_multiple_into_backend_duckdb_letsql(trino_table):
    db_con = ls.duckdb.connect()
    ls_con = ls.connect()

    expr = (
        trino_table.head(10_000)
        .pipe(into_backend, db_con)
        .pipe(make_merged)
        .pipe(into_backend, ls_con)[lambda t: t.orderstatus == "F"]
    )

    plan = segment(expr.op())
    node = transform_plan(plan)
    expr = node.to_expr()

    actual = expr.execute()

    assert isinstance(actual, pd.DataFrame)
    assert len(actual) > 0


def test_into_backend_duckdb_trino_cached(trino_table, tmp_path):
    db_con = ls.duckdb.connect()
    expr = (
        trino_table.head(10_000)
        .pipe(into_backend, db_con)
        .pipe(make_merged)
        .cache(ParquetCacheStorage(path=tmp_path))
    )

    plan = segment(expr.op())
    node = transform_plan(plan)
    expr = node.to_expr()

    actual = expr.execute()

    assert isinstance(actual, pd.DataFrame)
    assert len(actual) > 0
