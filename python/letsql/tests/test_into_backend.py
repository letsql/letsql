import pytest

import letsql as ls
from letsql.common.caching import SourceStorage

from letsql.expr.relations import into_backend

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


def test_into_backend(pg):
    con = ls.connect()

    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
        .cache(SourceStorage(source=con))
    )

    assert expr.execute() is not None
