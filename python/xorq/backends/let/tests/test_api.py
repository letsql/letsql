from pathlib import Path

import pytest

import xorq as xq


@pytest.fixture(scope="session")
def csv_dir():
    root = Path(__file__).absolute().parents[5]
    data_dir = root / "ci" / "ibis-testing-data" / "csv"
    return data_dir


@pytest.fixture(scope="session")
def parquet_dir():
    root = Path(__file__).absolute().parents[5]
    data_dir = root / "ci" / "ibis-testing-data" / "parquet"
    return data_dir


def test_register_read_csv(csv_dir):
    # this will use ls.options.backend: do we want to clear it out between function invocations?
    api_batting = xq.read_csv(csv_dir / "batting.csv", table_name="api_batting")
    result = xq.execute(api_batting)

    assert result is not None


def test_register_read_parquet(parquet_dir):
    # this will use ls.options.backend: do we want to clear it out between function invocations?
    api_batting = xq.read_parquet(
        parquet_dir / "batting.parquet", table_name="api_batting"
    )
    result = xq.execute(api_batting)

    assert result is not None


@pytest.mark.xfail(reason="No purpose with no registration api")
def test_executed_on_original_backend(parquet_dir, csv_dir, mocker):
    con = xq.config._backend_init()
    spy = mocker.spy(con, "execute")

    parquet_table = xq.read_parquet(parquet_dir / "batting.parquet")[
        lambda t: t.yearID == 2015
    ]

    csv_table = xq.read_csv(csv_dir / "batting.csv")[lambda t: t.yearID == 2014]

    expr = parquet_table.join(
        csv_table,
        "playerID",
    )

    assert xq.execute(expr) is not None
    assert spy.call_count == 1


def test_read_postgres():
    uri = "postgres://postgres:postgres@localhost:5432/ibis_testing"
    t = xq.read_postgres(uri, table_name="batting")
    res = xq.execute(t)

    assert res is not None and len(res)


@pytest.mark.xfail
def test_read_sqlite(tmp_path):
    import sqlite3

    xq.options.interactive = True
    db_path = tmp_path / "sqlite.db"
    with sqlite3.connect(db_path) as sq3:
        sq3.execute("DROP TABLE IF EXISTS t")
        sq3.execute("CREATE TABLE t (a INT, b TEXT)")
        sq3.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    t = xq.read_sqlite(path=db_path, table_name="t")
    res = xq.execute(t)

    assert res is not None and len(res)


@pytest.mark.parametrize(
    ("with_repartition_file_scans", "keep_partition_by_columns"),
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_with_config(
    with_repartition_file_scans, keep_partition_by_columns, parquet_dir
):
    import pandas as pd

    from xorq import SessionConfig

    session_config = (
        SessionConfig()
        .with_repartition_file_scans(with_repartition_file_scans)
        .set(
            "datafusion.execution.keep_partition_by_columns",
            str(keep_partition_by_columns).lower(),
        )
    )

    con = xq.connect(session_config=session_config)

    expr = con.read_parquet(parquet_dir / "batting.parquet").limit(10)
    result = expr.execute()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 10
