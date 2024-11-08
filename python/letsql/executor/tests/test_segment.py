import ibis

import letsql as ls
from letsql.common.caching import SourceStorage, ParquetCacheStorage
from letsql.executor.segment import segment
from letsql.expr.relations import into_backend


def _format(graph: list) -> list[str]:
    def name(obj):
        return obj.__class__.__name__

    result = []
    for val in graph:
        if isinstance(val, list):
            result.append(_format(val))
        else:
            result.append(name(val))

    return result


def test_segment(pg):
    con = ls.connect()
    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
    )

    actual = _format(segment(expr.op()))
    expected = ["NoneType", "NoneType", ["RemoteTable", "RemoteTable", "Limit"]]

    assert actual == expected


def test_segment_join(ddb_ages, sqlite_names):
    joined = ddb_ages.join(
        into_backend(sqlite_names, ddb_ages.op().source, name="ddb_names"), "id"
    ).select("name", "age")

    actual = _format(segment(joined.op()))
    expected = ["NoneType", "NoneType", ["RemoteTable", "JoinChain"]]

    assert actual == expected


def test_segment_union(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.union(
        into_backend(sqlite_names_timestamps, ddb_ages.op().source)
    )

    actual = _format(segment(result.op()))
    expected = ["NoneType", "NoneType", ["RemoteTable", "Union"]]

    assert actual == expected


def test_segment_intersect(ddb_ages, sqlite_names):
    ddb_ages_timestamps = ddb_ages.select("id")
    sqlite_names_timestamps = sqlite_names.select("id")

    result = ddb_ages_timestamps.intersect(
        into_backend(sqlite_names_timestamps, ddb_ages.op().source)
    )

    actual = _format(segment(result.op()))
    expected = ["NoneType", "NoneType", ["RemoteTable", "Intersection"]]

    assert actual == expected


def test_segment_asof_join(ddb_ages, sqlite_names):
    result = ddb_ages.asof_join(
        into_backend(sqlite_names, ddb_ages.op().source),
        on="timestamp",
        predicates="id",
    )

    actual = _format(segment(result.op()))
    expected = ["NoneType", "NoneType", ["RemoteTable", "JoinChain"]]

    assert actual == expected


def test_segment_into_backend_cache(pg):
    con = ls.connect()
    ddb_con = ibis.duckdb.connect()

    t = into_backend(pg.table("batting"), con, "ls_batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .limit(15)
        .cache(SourceStorage(source=ddb_con))
    )

    actual = _format(segment(expr.op()))
    expected = [
        ["NoneType", "NoneType", ["RemoteTable", "RemoteTable", "RemoteTable"]],
        "CachedNode",
        [],
    ]
    assert actual == expected


def test_segment_into_backend_multiple_cache(pg, tmp_path):
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

    actual = _format(segment(expr.op()))

    expected = [
        [
            ["NoneType", "NoneType", ["RemoteTable", "RemoteTable", "Limit"]],
            "CachedNode",
            ["RemoteTable", "Project"],
        ],
        "CachedNode",
        [],
    ]
    assert actual == expected
