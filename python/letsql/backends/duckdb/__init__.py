from typing import Mapping, Any

from ibis.backends.duckdb import Backend as IbisDuckDBBackend
from ibis.expr import types as ir

from letsql.backends.duckdb.compiler import DuckDBCompiler
from letsql.common.utils.graph_utils import replace_fix
from letsql.expr.relations import RemoteTableReplacer


class Backend(IbisDuckDBBackend):
    compiler = DuckDBCompiler()

    def execute(
        self,
        expr: ir.Expr,
        params: Mapping | None = None,
        limit: str | None = "default",
        **_: Any,
    ) -> Any:
        expr = expr.op().replace(replace_fix(RemoteTableReplacer())).to_expr()
        return super().execute(expr, params=params, limit=limit)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        params: Mapping | None = None,
        limit: str | None = "default",
        **kwargs: Any,
    ) -> Any:
        expr = expr.op().replace(replace_fix(RemoteTableReplacer())).to_expr()
        batches = super().to_pyarrow_batches(expr, params=params, limit=limit, **kwargs)
        return batches
