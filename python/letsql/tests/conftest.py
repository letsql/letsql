from pathlib import Path

import ibis
import numpy as np
import pandas as pd
import pytest

import letsql as ls

TEST_TABLES = {
    "functional_alltypes": ibis.schema(
        {
            "id": "int32",
            "bool_col": "boolean",
            "tinyint_col": "int8",
            "smallint_col": "int16",
            "int_col": "int32",
            "bigint_col": "int64",
            "float_col": "float32",
            "double_col": "float64",
            "date_string_col": "string",
            "string_col": "string",
            "timestamp_col": "timestamp",
            "year": "int32",
            "month": "int32",
        }
    ),
    "diamonds": ibis.schema(
        {
            "carat": "float64",
            "cut": "string",
            "color": "string",
            "clarity": "string",
            "depth": "float64",
            "table": "float64",
            "price": "int64",
            "x": "float64",
            "y": "float64",
            "z": "float64",
        }
    ),
    "batting": ibis.schema(
        {
            "playerID": "string",
            "yearID": "int64",
            "stint": "int64",
            "teamID": "string",
            "lgID": "string",
            "G": "int64",
            "AB": "int64",
            "R": "int64",
            "H": "int64",
            "X2B": "int64",
            "X3B": "int64",
            "HR": "int64",
            "RBI": "int64",
            "SB": "int64",
            "CS": "int64",
            "BB": "int64",
            "SO": "int64",
            "IBB": "int64",
            "HBP": "int64",
            "SH": "int64",
            "SF": "int64",
            "GIDP": "int64",
        }
    ),
    "awards_players": ibis.schema(
        {
            "playerID": "string",
            "awardID": "string",
            "yearID": "int64",
            "lgID": "string",
            "tie": "string",
            "notes": "string",
        }
    ),
    "astronauts": ibis.schema(
        {
            "id": "int64",
            "number": "int64",
            "nationwide_number": "int64",
            "name": "string",
            "original_name": "string",
            "sex": "string",
            "year_of_birth": "int64",
            "nationality": "string",
            "military_civilian": "string",
            "selection": "string",
            "year_of_selection": "int64",
            "mission_number": "int64",
            "total_number_of_missions": "int64",
            "occupation": "string",
            "year_of_mission": "int64",
            "mission_title": "string",
            "ascend_shuttle": "string",
            "in_orbit": "string",
            "descend_shuttle": "string",
            "hours_mission": "float64",
            "total_hrs_sum": "float64",
            "field21": "int64",
            "eva_hrs_mission": "float64",
            "total_eva_hrs": "float64",
        }
    ),
}

array_types_df = pd.DataFrame(
    [
        (
            [np.int64(1), 2, 3],
            ["a", "b", "c"],
            [1.0, 2.0, 3.0],
            "a",
            1.0,
            [[], [np.int64(1), 2, 3], None],
        ),
        (
            [4, 5],
            ["d", "e"],
            [4.0, 5.0],
            "a",
            2.0,
            [],
        ),
        (
            [6, None],
            ["f", None],
            [6.0, np.nan],
            "a",
            3.0,
            [None, [], None],
        ),
        (
            [None, 1, None],
            [None, "a", None],
            [],
            "b",
            4.0,
            [[1], [2], [], [3, 4, 5]],
        ),
        (
            [2, None, 3],
            ["b", None, "c"],
            np.nan,
            "b",
            5.0,
            None,
        ),
        (
            [4, None, None, 5],
            ["d", None, None, "e"],
            [4.0, np.nan, np.nan, 5.0],
            "c",
            6.0,
            [[1, 2, 3]],
        ),
    ],
    columns=[
        "x",
        "y",
        "z",
        "grouper",
        "scalar_column",
        "multi_dim",
    ],
)


@pytest.fixture(scope="session")
def data_dir():
    root = Path(__file__).absolute().parents[3]
    data_dir = root / "ci" / "ibis-testing-data"
    return data_dir


@pytest.fixture(scope="session")
def ddl_file():
    root = Path(__file__).absolute().parents[3]
    ddl_dir = root / "db" / "datafusion.sql"
    return ddl_dir


def statements(ddl_file: Path):
    return (
        statement
        for statement in map(str.strip, ddl_file.read_text().split(";"))
        if statement
    )


@pytest.fixture(scope="session")
def con(data_dir, ddl_file):
    conn = ls.connect()
    parquet_dir = data_dir / "parquet"
    conn.register(parquet_dir / "functional_alltypes.parquet", "functional_alltypes")
    conn.register(parquet_dir / "batting.parquet", "batting")
    conn.register(parquet_dir / "diamonds.parquet", "diamonds")
    conn.register(parquet_dir / "astronauts.parquet", "astronauts")
    conn.register(parquet_dir / "awards_players.parquet", "awards_players")

    conn.register(array_types_df, "array_types")

    if ddl_file.is_file() and ddl_file.name.endswith(".sql"):
        for statement in statements(ddl_file):
            with conn._safe_raw_sql(statement):  # noqa
                pass

    return conn


@pytest.fixture(scope="session")
def functional_alltypes(con):
    return con.table("functional_alltypes")


@pytest.fixture(scope="session")
def alltypes(con):
    return con.table("functional_alltypes")


@pytest.fixture(scope="session")
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope="session")
def batting(con):
    return con.table("batting")


@pytest.fixture(scope="session")
def batting_df(batting):
    return batting.execute()


@pytest.fixture(scope="session")
def awards_players(con):
    return con.table("awards_players")


@pytest.fixture(scope="session")
def awards_players_df(awards_players):
    return awards_players.execute(limit=None)


@pytest.fixture(scope="session")
def sorted_df(df):
    return df.sort_values("id").reset_index(drop=True)


@pytest.fixture(scope="session")
def diamonds(con):
    return con.table("diamonds")


@pytest.fixture(scope="session")
def array_types(con):
    return con.table("array_types")


@pytest.fixture(scope="session")
def struct(con):
    return con.table("structs")


@pytest.fixture(scope="session")
def struct_df(struct):
    return struct.execute()
