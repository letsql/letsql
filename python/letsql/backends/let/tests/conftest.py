import pathlib

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

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


@pytest.fixture(scope="session")
def pg():
    conn = ls.postgres.connect(
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
    return pg


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


@pytest.fixture(scope="function")
def con(dirty):
    remove_unexpected_tables(dirty)
    yield dirty
    # cleanup
    remove_unexpected_tables(dirty)


@pytest.fixture(scope="session")
def dirty_ls_con():
    con = ls.connect()
    return con


@pytest.fixture(scope="function")
def ls_con(dirty_ls_con):
    # since we don't register, maybe just create a fresh con
    yield dirty_ls_con
    # drop tables
    for table_name in dirty_ls_con.list_tables():
        dirty_ls_con.drop_table(table_name, force=True)
    # drop view
    for table_name in dirty_ls_con.list_tables():
        dirty_ls_con.drop_view(table_name, force=True)


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


@pytest.fixture(scope="session")
def batting_df(batting):
    return batting.execute()


@pytest.fixture
def df():
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([0, 1, 2, 3, 4, 5, 6]),
            pa.array([7, 4, 3, 8, 9, 1, 6]),
            pa.array(["A", "A", "A", "A", "B", "B", "B"]),
        ],
        names=["a", "b", "c"],
    )

    return batch.to_pandas()


@pytest.fixture(scope="function")
def parquet_metadata():
    return {b"mykey": b"myvalue"}


@pytest.fixture(scope="function")
def parquet_path_without_metadata(tmpdir):
    parquet_path_without_metadata = pathlib.Path(tmpdir).joinpath(
        "without-metadata.parquet"
    )
    metadata = {b"mykey": b"myvalue"}
    table = pa.Table.from_pydict({"a": [1], "b": ["two"]}).replace_schema_metadata(
        metadata
    )
    with pq.ParquetWriter(parquet_path_without_metadata, table.schema) as writer:
        writer.write_table(table)
    return parquet_path_without_metadata


@pytest.fixture(scope="function")
def parquet_path_with_metadata(tmpdir):
    parquet_path_with_metadata = pathlib.Path(tmpdir).joinpath("with-metadata.parquet")
    metadata = {b"mykey": b"myvalue"}
    table = pa.Table.from_pydict({"a": [1], "b": ["two"]}).replace_schema_metadata(
        metadata
    )
    # order of sorts matters!!!
    sort_order = [(el.name, "ascending") for el in table.schema][::-1]
    sorting_columns = pq.SortingColumn.from_ordering(table.schema, sort_order)
    with pq.ParquetWriter(
        parquet_path_with_metadata, table.schema, sorting_columns=sorting_columns
    ) as writer:
        writer.write_table(table)
    return parquet_path_with_metadata
