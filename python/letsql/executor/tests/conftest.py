from datetime import datetime, timedelta

import ibis
import pandas as pd
import pytest

import letsql as ls

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


@pytest.fixture(scope="session")
def pg():
    conn = ls.postgres.connect_env()
    yield conn
    remove_unexpected_tables(conn)


@pytest.fixture(scope="session")
def trino_table():
    return (
        ibis.trino.connect(database="tpch", schema="sf1")
        .table("orders")
        .cast({"orderdate": "date"})
    )


@pytest.fixture(scope="session")
def sqlite_names():
    # Connect to SQLite
    sqlite_con = ibis.sqlite.connect(
        ":memory:"
    )  # Using in-memory database for this example

    sqlite_data = pd.DataFrame(
        {
            "id": range(1, 6),
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "timestamp": [datetime.now() + timedelta(hours=i) for i in range(5)],
        }
    )

    return sqlite_con.create_table("users", sqlite_data)


@pytest.fixture(scope="session")
def ddb_ages():
    # Connect to DuckDB
    ddb_con = ibis.duckdb.connect()

    ddb_data = pd.DataFrame(
        {
            "id": range(4, 9),
            "age": [30, 35, 40, 45, 50],
            "timestamp": [datetime.now() + timedelta(hours=i) for i in range(5)],
        }
    )

    return ddb_con.create_table("ddb_users", ddb_data)
