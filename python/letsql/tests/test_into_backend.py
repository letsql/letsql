import pandas as pd
import pytest

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
    import ibis

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
    assert 0 < len(res) <= 15
