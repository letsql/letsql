import pandas as pd

import letsql as ls
from letsql.common.caching import SourceStorage
from letsql.executor import execute


def test_join(ddb_ages, sqlite_names):
    joined = ddb_ages.join(sqlite_names, "id").select("name", "age")

    result = joined.pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_union(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.union(sqlite_names_timestamps).pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_intersect(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.intersect(sqlite_names_timestamps).pipe(execute)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_asof_join(ddb_ages, sqlite_names):
    result = ddb_ages.asof_join(sqlite_names, on="timestamp", predicates="id").pipe(
        execute
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_cross_backend_cache(pg):
    con = ls.duckdb.connect()
    expr = (
        pg.table("batting")
        .cache(SourceStorage(con))
        .limit(15)
        .select("playerID", "yearID")
    )

    res = execute(expr)

    assert isinstance(res, pd.DataFrame)
    assert len(res) == 15
