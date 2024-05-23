import os
from contextlib import contextmanager
from typing import Any

import ibis
import pandas as pd
import pandas.testing as tm
import pytest
import sqlglot as sg
import sqlglot.expressions as sge
from ibis.util import gen_name

import letsql as ls
import letsql.common.utils.snowflake_utils as SU
from letsql.expr.relations import CachedNode


snowflake_credentials_varnames = (
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_USER",
)
have_snowflake_credentials = all(
    os.environ.get(varname) for varname in snowflake_credentials_varnames
)


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


def pytest_runtest_setup(item):
    if any(mark.name == "snowflake" for mark in item.iter_markers()):
        pytest.importorskip("snowflake.connector")
        if not have_snowflake_credentials:
            pytest.skip("cannot run snowflake tests without snowflake creds")


def get_storage_uncached(con, expr):
    op = expr.op()
    assert isinstance(op, CachedNode)

    def replace_table(node, _, **_kwargs):
        return con._sources.get_table(node, node.__recreate__(_kwargs))

    uncached = expr.op().replace(replace_table).parent.to_expr()
    return (op.storage, uncached)


@pytest.fixture(scope="session")
def pg():
    conn = ibis.postgres.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="ibis_testing",
    )
    yield conn
    remove_unexpected_tables(conn)


@pytest.fixture(scope="session")
def dirty(pg):
    con = ls.connect()

    for table_name in expected_tables:
        con.register(pg.table(table_name), table_name=table_name)

    return con


def remove_unexpected_tables(dirty):
    for table in dirty.list_tables():
        if table not in expected_tables:
            dirty.drop_table(table)
    if sorted(dirty.list_tables()) != sorted(expected_tables):
        raise ValueError


@pytest.fixture(scope="function")
def con(dirty):
    remove_unexpected_tables(dirty)
    yield dirty
    # cleanup
    remove_unexpected_tables(dirty)


@pytest.fixture(scope="session")
def alltypes(dirty):
    return dirty.table("functional_alltypes")


@pytest.fixture(scope="session")
def batting(dirty):
    return dirty.table("batting")


@pytest.fixture(scope="session")
def awards_players(dirty):
    return dirty.table("awards_players")


@pytest.fixture(scope="session")
def alltypes_df(alltypes):
    return alltypes.execute()


def assert_frame_equal(
    left: pd.DataFrame, right: pd.DataFrame, *args: Any, **kwargs: Any
) -> None:
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)
    kwargs.setdefault("check_dtype", True)
    tm.assert_frame_equal(left, right, *args, **kwargs)


@pytest.fixture(scope="session")
def sf_con():
    return ibis.snowflake.connect(
        # a database/schema we can trust exists
        database="SNOWFLAKE_SAMPLE_DATA",
        schema="TPCH_SF1",
        **SU.make_credential_defaults(),
        **SU.make_connection_defaults(),
        create_object_udfs=False,
    )


@pytest.fixture
def temp_catalog(sf_con):
    cat = gen_name("tmp_catalog")

    sf_con.create_catalog(cat)
    assert cat in sf_con.list_catalogs()

    yield cat

    sf_con.drop_catalog(cat)
    assert cat not in sf_con.list_catalogs()


@pytest.fixture
def temp_db(sf_con, temp_catalog):
    database = gen_name("tmp_database")

    sf_con.create_database(database, catalog=temp_catalog)
    assert database in sf_con.list_databases(catalog=temp_catalog)

    yield database

    sf_con.drop_database(database, catalog=temp_catalog)
    assert database not in sf_con.list_databases(catalog=temp_catalog)


@contextmanager
def inside_temp_schema(con, temp_catalog, temp_db):
    (prev_catalog, prev_db) = (con.current_catalog, con.current_database)
    con.raw_sql(
        sge.Use(
            kind="SCHEMA", this=sg.table(temp_db, db=temp_catalog, quoted=True)
        ).sql(dialect=con.name),
    )
    yield
    con.raw_sql(
        sge.Use(
            kind="SCHEMA", this=sg.table(prev_db, db=prev_catalog, quoted=True)
        ).sql(dialect=con.name),
    )
