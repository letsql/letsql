import ibis
import pandas as pd
import pytest
from ibis import _ as __

import letsql as ls
from letsql.common.caching import SourceStorage
from letsql.expr.relations import into_backend, RemoteTableReplacer

expected_tables = (
    "array_types",
    "astronauts",
    "awards_players",
    "awards_players_special_types",
    "batting",
    "diamonds",
    "functional_alltypes",
    "geo",
    "geography_columns",
    "geometry_columns",
    "json_t",
    "map",
    "spatial_ref_sys",
    "topk",
    "tzone",
)


@pytest.fixture(scope="session")
def pg():
    conn = ls.postgres.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="ibis_testing",
    )
    yield conn
    remove_unexpected_tables(conn)


def remove_unexpected_tables(dirty):
    # drop tables
    for table in dirty.list_tables():
        if table not in expected_tables:
            dirty.drop_table(table, force=True)

    # drop view
    for table in dirty.list_tables():
        if table not in expected_tables:
            dirty.drop_view(table, force=True)

    if sorted(dirty.list_tables()) != sorted(expected_tables):
        raise ValueError


def test_multiple_record_batches(pg):
    con = ls.connect()

    table = pg.table("batting")
    left = con.register(table.to_pyarrow_batches(), "batting_0")
    right = con.register(table.to_pyarrow_batches(), "batting_1")

    expr = (
        left.join(right, "playerID")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
        .cache(SourceStorage(source=con))
    )

    res = expr.execute()
    assert isinstance(res, pd.DataFrame)
    assert 0 < len(res) <= 15


def test_into_backend(pg):
    con = ls.connect()

    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
        .cache(SourceStorage(source=con))
    )

    assert ls.to_sql(expr).count("ls_batting") == 2
    res = expr.execute()
    assert 0 < len(res) <= 15


def test_into_backend_duckdb(pg):
    ddb = ibis.duckdb.connect()
    t = into_backend(pg.table("batting"), ddb, "ls_batting")
    expr = (
        t.join(t, "playerID")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )

    expr = expr.op().replace(RemoteTableReplacer()).to_expr()
    query = ibis.to_sql(expr, dialect="duckdb")

    res = ddb.con.sql(query).df()

    assert query.count("ls_batting") == 2
    assert 0 < len(res) <= 15


def test_into_backend_duckdb_expr(pg):
    ddb = ibis.duckdb.connect()
    t = into_backend(pg.table("batting"), ddb, "ls_batting")
    expr = t.join(t, "playerID").limit(15).select(__.playerID * 2)

    expr = expr.op().replace(RemoteTableReplacer()).to_expr()
    query = ibis.to_sql(expr, dialect="duckdb")

    res = ddb.con.sql(query).df()

    assert query.count("ls_batting") == 2
    assert 0 < len(res) <= 15


@pytest.fixture(scope="session")
def trino_table():
    return (
        ibis.trino.connect(database="tpch", schema="sf1")
        .table("orders")
        .cast({"orderdate": "date"})
    )


def make_merged(expr):
    agg = expr.group_by(["custkey", "orderdate"]).agg(
        __.totalprice.sum().name("totalprice")
    )
    w = ibis.window(group_by="custkey", order_by="orderdate")
    windowed = (
        agg.mutate(__.totalprice.cumsum().over(w).name("curev"))
        .mutate(__.curev.lag(1).over(w).name("curev@t-1"))
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


def test_into_backend_duckdb_trino(trino_table):
    db_con = ibis.duckdb.connect()
    expr = trino_table.head(10_000).pipe(into_backend, db_con).pipe(make_merged)

    expr = expr.op().replace(RemoteTableReplacer()).to_expr()
    query = ibis.to_sql(expr, dialect="duckdb")

    df = db_con.con.sql(query).df()  # to bypass execute hotfix

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
