from pathlib import Path

import pytest

import letsql


from ibis.backends.base.sql.compiler import Compiler, QueryContext


def get_query(expr):
    ast = Compiler.to_ast(expr, QueryContext(compiler=Compiler))
    return ast.queries[0]


def to_sql(expr, *args, **kwargs) -> str:
    return get_query(expr).compile(*args, **kwargs)


@pytest.fixture(scope="session")
def data_dir():
    root = Path(__file__).absolute().parents[4]
    data_dir = root / "ci" / "ibis-testing-data"
    return data_dir


@pytest.fixture(scope="session")
def con(data_dir):
    conn = letsql.con()
    parquet_dir = data_dir / "parquet"
    conn.register(parquet_dir / "functional_alltypes.parquet", "functional_alltypes")

    return conn


@pytest.fixture(scope="session")
def alltypes(con):
    return con.table("functional_alltypes")


@pytest.fixture(scope="session")
def df(alltypes):
    return alltypes.execute()
