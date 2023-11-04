from __future__ import annotations

import gzip
import pytest


@pytest.fixture
def gzip_csv(data_dir, tmp_path):
    basename = "diamonds.csv"
    f = tmp_path.joinpath(f"{basename}.gz")
    data = data_dir.joinpath("csv", basename).read_bytes()
    f.write_bytes(gzip.compress(data))
    return str(f.absolute())


def test_register_csv(con, data_dir):
    fname = "diamonds.csv"
    table_name = "diamonds"
    table = con.register(data_dir / "csv" / fname, table_name=table_name)
    assert any(table_name in t for t in con.list_tables())
    assert table.count().execute() > 0


def test_register_csv_gz(con, data_dir, gzip_csv):
    table = con.register(gzip_csv, table_name="diamonds")
    assert table.count().execute() > 0


def test_register_with_dotted_name(con, data_dir, tmp_path):
    basename = "foo.bar.baz/diamonds.csv"
    f = tmp_path.joinpath(basename)
    f.parent.mkdir()
    data = data_dir.joinpath("csv", "diamonds.csv").read_bytes()
    f.write_bytes(data)
    table = con.register(str(f.absolute()), table_name="diamonds")
    assert table.count().execute() > 0


def test_register_parquet(con, data_dir):
    fname = "functional_alltypes.parquet"
    table_name = "funk_all"
    table = con.register(data_dir / "parquet" / fname, table_name=table_name)

    assert any(table_name in t for t in con.list_tables())
    assert table.count().execute() > 0
