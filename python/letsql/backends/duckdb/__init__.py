from ibis.backends.duckdb import Backend as IbisDuckDBBackend

from letsql.backends.duckdb.compiler import DuckDBCompiler


class Backend(IbisDuckDBBackend):
    compiler = DuckDBCompiler()
