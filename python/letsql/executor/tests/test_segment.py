import numpy as np
import pandas as pd

from letsql.common.caching import SourceStorage, ParquetCacheStorage
from letsql.executor.segment import execute
from letsql.tests.util import assert_frame_equal


def cached_tables(con) -> tuple:
    return tuple(
        table_name
        for table_name in con.list_tables()
        if table_name.startswith("letsql_cache")
    )


def _pandas_semi_join(left, right, on, **_):
    assert len(on) == 1, str(on)
    inner = pd.merge(left, right, how="inner", on=on)
    filt = left.loc[:, on[0]].isin(inner.loc[:, on[0]])
    return left.loc[filt, :]


def _pandas_anti_join(left, right, on, **_):
    inner = pd.merge(left, right, how="left", indicator=True, on=on)
    return inner[inner["_merge"] == "left_only"]


IMPLS = {
    "semi": _pandas_semi_join,
    "anti": _pandas_anti_join,
}


def check_eq(left, right, how, **kwargs):
    impl = IMPLS.get(how, pd.merge)
    return impl(left, right, how=how, **kwargs)


def test_segment_transform_simple_expr(pg):
    t = pg.table("batting")

    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
    )

    actual = execute(expr)
    expected = expr.execute()

    assert_frame_equal(actual, expected)


def test_segment_one_cache(pg):
    t = pg.table("batting")
    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
    )

    # same backend storage
    cached_expr = expr.cache(SourceStorage(pg))

    assert not cached_tables(pg)

    actual = execute(cached_expr)
    expected = expr.execute()

    assert cached_tables(pg)

    assert_frame_equal(actual, expected)


def assert_two_caches(cached_expr, pg, t, tmp_path):
    expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
    )
    assert not cached_tables(pg)
    actual = execute(cached_expr)
    expected = expr.execute()
    assert (table_names := cached_tables(pg))
    assert any(
        (tmp_path / f"{table_name}.parquet").exists() for table_name in table_names
    )  # at least one is Parquet
    assert_frame_equal(actual, expected)


def test_segment_two_cache(pg, tmp_path):
    t = pg.table("batting")
    cached_expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .cache(ParquetCacheStorage(pg, tmp_path))
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
        .cache(SourceStorage(pg))
    )

    assert_two_caches(cached_expr, pg, t, tmp_path)


def test_segment_two_cache_no_middle_expr(pg, tmp_path):
    t = pg.table("batting")
    cached_expr = (
        t.join(t, "playerID")
        .order_by("playerID", "yearID_right")
        .select(player_id="playerID", year_id="yearID_right")
        .limit(15)
        .cache(ParquetCacheStorage(pg, tmp_path))
        .cache(SourceStorage(pg))
    )

    assert_two_caches(cached_expr, pg, t, tmp_path)


def test_segment_two_cache_in_join(pg):
    # FIXME Bug with ParquetStorage when reading existing cache
    batting_table = pg.table("batting").cache(SourceStorage(pg))
    awards_players_table = pg.table("awards_players").cache(SourceStorage(pg))

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
