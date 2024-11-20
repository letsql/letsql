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
def limit_tables(draw, ts=tables_strategy):
    table = draw(ts)
    length = table.count().execute()
    n = draw(st.integers(min_value=0, max_value=length))
    offset = draw(st.integers(min_value=0, max_value=length - n))

    return table.limit(n=n, offset=offset)


@st.composite
def project_tables(draw, ts=tables_strategy):
    table = draw(ts)
    schema = list(table.schema())
    projections = st.sets(st.sampled_from(schema), min_size=1, max_size=len(schema))
    return table.select(*draw(projections))


@settings(deadline=5000)
@given(table=tables_strategy)
def test_tables(table):
    assert ls.execute(table) is not None


@settings(deadline=5000, max_examples=50)
@given(expr=limit_tables())
def test_limit(expr):
    assert ls.execute(expr) is not None


@settings(deadline=5000, max_examples=50)
@given(expr=project_tables())
def test_project(expr):
    assert ls.execute(expr) is not None
