from datetime import datetime, timedelta

import ibis
import pandas as pd
import pytest


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
