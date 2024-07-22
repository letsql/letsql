from pathlib import Path

import letsql as ls
import pytest

import letsql.backends.let
from letsql.common.caching import ParquetCacheStorage


@pytest.fixture(scope="session")
def parquet_dir():
    root = Path(__file__).absolute().parents[4]
    data_dir = root / "ci" / "ibis-testing-data" / "parquet"
    return data_dir


def test_put_get_drop(tmp_path, parquet_dir):
    astronauts_path = parquet_dir.joinpath("astronauts.parquet")

    con = ls.datafusion.connect()
    t = con.register(astronauts_path, table_name="astronauts")

    storage = ParquetCacheStorage(path=tmp_path, source=con)
    put_node = storage.put(t, t.op())
    assert put_node is not None

    get_node = storage.get(t)
    assert get_node is not None

    storage.drop(t)
    with pytest.raises(KeyError):
        storage.get(t)


def test_default_connection(tmp_path, parquet_dir):
    batting_path = parquet_dir.joinpath("astronauts.parquet")

    con = ls.datafusion.connect()
    t = con.register(batting_path, table_name="astronauts")

    storage = ParquetCacheStorage(path=tmp_path)
    storage.put(t, t.op())

    get_node = storage.get(t)
    assert get_node is not None
    assert get_node.source.name == "let"
    assert letsql.options.backend is not None
    assert get_node.to_expr().execute is not None
