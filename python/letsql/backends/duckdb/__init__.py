from typing import Mapping, Any

from ibis.backends.duckdb import Backend as IbisDuckDBBackend
from ibis.expr import types as ir

from letsql.backends.duckdb.compiler import DuckDBCompiler
from letsql.expr.relations import register_and_transform_remote_tables


class Backend(IbisDuckDBBackend):
    compiler = DuckDBCompiler()

    def execute(
        self,
        expr: ir.Expr,
        params: Mapping | None = None,
        limit: str | None = "default",
        **_: Any,
    ) -> Any:
        expr, _ = register_and_transform_remote_tables(expr)
        return super().execute(expr, params=params, limit=limit)
