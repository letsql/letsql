from pathlib import Path

import pytest

import letsql


@pytest.fixture(scope="session")
def data_dir():
    root = Path(__file__).absolute().parents[3]
    data_dir = root / "ci" / "ibis-testing-data"
    return data_dir


@pytest.fixture(scope="session")
def con(data_dir):
    conn = letsql.con()
    parquet_dir = data_dir / "parquet"
    conn.register(parquet_dir / "functional_alltypes.parquet", "functional_alltypes")
    conn.register(parquet_dir / "batting.parquet", "batting")
    conn.register(parquet_dir / "diamonds.parquet", "diamonds")
    conn.register(parquet_dir / "astronauts.parquet", "astronauts")
    conn.register(parquet_dir / "awards_players.parquet", "awards_players")

    return conn


@pytest.fixture(scope="session")
def functional_alltypes(con):
    return con.table("functional_alltypes")


@pytest.fixture(scope="session")
def alltypes(con):
    return con.table("functional_alltypes")
