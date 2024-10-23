import pandas as pd

from letsql.executor import collect


def test_join(ddb_ages, sqlite_names):
    joined = ddb_ages.join(sqlite_names, "id").select("name", "age")

    result = joined.pipe(collect)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_union(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.union(sqlite_names_timestamps).pipe(collect)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_intersect(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.intersect(sqlite_names_timestamps).pipe(collect)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_asof_join(ddb_ages, sqlite_names):
    result = ddb_ages.asof_join(sqlite_names, on="timestamp", predicates="id").pipe(
        collect
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
