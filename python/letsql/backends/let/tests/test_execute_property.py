from functools import cache, partial
from pathlib import Path

import letsql as ls

from hypothesis import given, settings, strategies as st

from ibis import Table


@cache
def tables():
    con = ls.connect()
    ddb = ls.duckdb.connect()
    pg = ls.postgres.connect_env()

    root = Path(__file__).absolute().parents[5]
    csv_dir = root / "ci" / "ibis-testing-data" / "csv"

    return [
        con.read_csv(csv_dir / "awards_players.csv", table_name="ls_players"),
        con.read_csv(csv_dir / "batting.csv", table_name="ls_batting"),
        ddb.read_csv(csv_dir / "awards_players.csv", table_name="ddb_players"),
        ddb.read_csv(csv_dir / "batting.csv", table_name="ddb_batting"),
        pg.table("awards_players"),
        pg.table("batting"),
    ]


tables_strategy = st.sampled_from(tables())

limit_strategy = st.sampled_from(
    [
        partial(Table.limit, n=10),
        partial(Table.limit, n=10, offset=5),
    ]
)


@st.composite
def limit_tables(draw, ts=tables_strategy, li=limit_strategy):
    table = draw(ts)
    limit = draw(li)
    return limit(table)


@settings(deadline=5000)
@given(table=tables_strategy)
def test_tables(table):
    assert ls.execute(table) is not None


@settings(deadline=5000)
@given(expr=limit_tables())
def test_limit(expr):
    assert ls.execute(expr) is not None
