import re

import numpy as np
import pandas as pd

import ibis
import pytest

import letsql as ls
from letsql.backends.let.tests.test_execute import check_eq
from letsql.common.caching import SourceStorage, ParquetCacheStorage
from letsql.executor.core import execute
from letsql.tests.util import assert_frame_equal
from letsql.expr.relations import into_backend, RemoteTableReplacer


def test_join(ddb_ages, sqlite_names):
    joined = ddb_ages.join(
        into_backend(sqlite_names, ddb_ages.op().source), "id"
    ).select("name", "age")

    result = joined.pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_union(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.union(
        into_backend(sqlite_names_timestamps, ddb_ages.op().source)
    ).pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_intersect(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.intersect(
        into_backend(sqlite_names_timestamps, ddb_ages.op().source)
    ).pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_asof_join(ddb_ages, sqlite_names):
    result = ddb_ages.asof_join(
        into_backend(sqlite_names, ddb_ages.op().source),
        on="timestamp",
        predicates="id",
    ).pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_join_alltypes(alltypes, alltypes_df):
    con = ibis.duckdb.connect()

    first_10 = alltypes_df.head(10)
    in_memory = con.register(first_10, table_name="in_memory")

    expr = into_backend(alltypes, con).join(in_memory, "id")
    actual = execute(expr).sort_values("id")
    expected = pd.merge(
        alltypes_df, first_10, how="inner", on="id", suffixes=("", "_right")
    ).sort_values("id")

    assert_frame_equal(actual, expected, check_dtype=False)


@pytest.mark.parametrize("how", ["semi", "anti"])
def test_filtering_join(batting, awards_players, how):
    left = batting[batting.yearID == 2015]
    right = awards_players[awards_players.lgID == "NL"].drop("yearID", "lgID")

    left_df = execute(left)
    right_df = execute(right)
    predicate = ["playerID"]
    result_order = ["playerID", "yearID", "lgID", "stint"]

    expr = left.join(right, predicate, how=how)
    result = (
        expr.pipe(execute)
        .fillna(np.nan)
        .sort_values(result_order)[left.columns]
        .reset_index(drop=True)
    )

    expected = check_eq(
        left_df,
        right_df,
        how=how,
        on=predicate,
        suffixes=("", "_y"),
    ).sort_values(result_order)[list(left.columns)]

    assert_frame_equal(result, expected, check_like=True)


def test_register_arbitrary_expression(batting):
    duckdb_con = ibis.duckdb.connect()

    expr = batting.filter(batting.playerID == "allisar01")[
        ["playerID", "yearID", "stint", "teamID", "lgID"]
    ]
    expected = execute(expr)
    result = execute(into_backend(expr, duckdb_con))

    assert result is not None
    assert_frame_equal(result, expected, check_like=True)


def test_arbitrary_expression_multiple_tables(pg):
    duckdb_con = ibis.duckdb.connect()

    batting = pg.table("batting")
    batting_table = into_backend(batting, duckdb_con)

    players_table = pg.table("awards_players")
    awards_players_table = into_backend(players_table, duckdb_con)

    left = batting_table[batting_table.yearID == 2015]
    right = awards_players_table[awards_players_table.lgID == "NL"].drop(
        "yearID", "lgID"
    )

    left_df = execute(left)
    right_df = execute(right)
    predicate = ["playerID"]
    result_order = ["playerID", "yearID", "lgID", "stint"]

    expr = left.join(right, predicate, how="inner")

    result = (
        execute(expr)
        .fillna(np.nan)
        .sort_values(result_order)[left.columns]
        .reset_index(drop=True)
    )

    expected = check_eq(
        left_df,
        right_df,
        how="inner",
        on=predicate,
        suffixes=("", "_y"),
    ).sort_values(result_order)[list(left.columns)]

    assert_frame_equal(result, expected, check_like=True)


@pytest.mark.parametrize(
    "new_con",
    [
        ibis.datafusion.connect(),
        ibis.duckdb.connect(),
    ],
)
def test_multiple_pipes(pg, new_con):
    """Originally reported on bug #69
    link: https://github.com/letsql/letsql/issues/69
    """

    table_name = "batting"
    pg_t = pg.table(table_name)[lambda t: t.yearID == 2015]
    db_t = new_con.register(pg_t.to_pyarrow(), f"db-{table_name}")[
        lambda t: t.yearID == 2014
    ]

    expr = pg_t.join(
        into_backend(db_t, pg),
        "playerID",
    )

    assert execute(expr) is not None


def test_native_execution(pg, mocker):
    table_name = "batting"
    spy = mocker.spy(pg, "execute")

    pg_t = pg.table(table_name)[lambda t: t.yearID == 2015]
    db_t = pg.table(table_name)[lambda t: t.yearID == 2014]

    expr = pg_t.join(
        db_t,
        "playerID",
    )

    assert execute(expr) is not None
    assert spy.call_count == 1


def test_no_registration_same_table_name(batting):
    ddb_con = ibis.duckdb.connect()
    ls_con = ibis.datafusion.connect()

    ddb_batting = ddb_con.register(
        batting[["playerID", "yearID"]].to_pyarrow_batches(), "batting"
    )
    ls_batting = ls_con.register(batting[["playerID", "stint"]].to_pyarrow(), "batting")

    expr = ddb_batting.join(
        into_backend(ls_batting, ddb_con),
        "playerID",
    )

    assert execute(expr) is not None


def test_into_backend(pg):
    ddb_con = ibis.duckdb.connect()

    t = into_backend(pg.table("batting"), ddb_con, "batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    actual = execute(expr)

    pg_t = pg.table("batting")
    pg_expr = (
        pg_t.join(pg_t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    expected = pg_expr.execute()

    assert_frame_equal(actual, expected)


def test_into_backend_cache(pg, tmp_path):
    con = ls.connect()
    ddb_con = ibis.duckdb.connect()

    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .cache(SourceStorage(source=con))
        .pipe(into_backend, ddb_con)
        .select(player_id="playerID", year_id="yearID_right")
        .cache(ParquetCacheStorage(source=ddb_con, path=tmp_path))
    )
    actual = execute(expr)

    pg_t = pg.table("batting")
    pg_expr = (
        pg_t.join(pg_t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )
    expected = pg_expr.execute()

    assert_frame_equal(actual, expected)


def test_into_backend_duckdb(pg):
    ddb = ibis.duckdb.connect()
    t = into_backend(pg.table("batting"), ddb, "ls_batting")
    expr = (
        t.join(t, "playerID")
        .limit(15)
        .select(player_id="playerID", year_id="yearID_right")
    )

    replacer = RemoteTableReplacer()
    expr = expr.op().replace(replacer).to_expr()
    query = ibis.to_sql(expr, dialect="duckdb")

    res = ddb.con.sql(query).df()

    assert len(re.findall(r"\d+_ls_batting", query)) == 2
    assert 0 < len(res) <= 15
    assert len(replacer.created) == 3


def test_cross_backend_cache(pg):
    con = ibis.duckdb.connect()

    expr = (
        pg.table("batting")
        .cache(SourceStorage(con))
        .order_by("playerID", "yearID")
        .select("playerID", "stint")
        .limit(15)
    )

    actual = execute(expr)

    pg_expr = (
        pg.table("batting")
        .order_by("playerID", "yearID")
        .select("playerID", "stint")
        .limit(15)
    )

    expected = pg_expr.execute()
    assert_frame_equal(actual, expected)


def test_cross_backend_cache_parquet(pg, tmp_path):
    con = ibis.duckdb.connect()

    expr = (
        pg.table("batting")
        .cache(ParquetCacheStorage(source=con, path=tmp_path))
        .order_by("playerID", "yearID")
        .select("playerID", "stint")
        .limit(15)
    )

    actual = execute(expr)

    pg_expr = (
        pg.table("batting")
        .order_by("playerID", "yearID")
        .select("playerID", "stint")
        .limit(15)
    )

    expected = pg_expr.execute()
    assert_frame_equal(actual, expected)