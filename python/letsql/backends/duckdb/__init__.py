import uuid
from collections import defaultdict
from typing import Mapping, Any

from ibis.common.graph import Graph
from ibis.expr import types as ir
from ibis.backends.duckdb import Backend as IbisDuckDBBackend

from letsql.common.caching import SourceStorage
from letsql.expr.relations import CachedNode

import ibis.expr.operations as ops

from letsql.backends.let import SESSION_ID_PREFIX


class Backend(IbisDuckDBBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session_id = f"{SESSION_ID_PREFIX}{uuid.uuid4().hex}_"

    @staticmethod
    def _register_and_transform_cache_tables(expr):
        """This function will sequentially execute any cache node that is not already cached"""

        def fn(node, _, **kwargs):
            node = node.__recreate__(kwargs)
            if isinstance(node, CachedNode):
                uncached, storage = node.parent, node.storage
                uncached_to_expr = uncached.to_expr()
                node = storage.set_default(uncached_to_expr, uncached)
            return node

        op = expr.op()
        out = op.replace(fn)

        return out.to_expr()

    def execute(
        self,
        expr: ir.Expr,
        params: Mapping | None = None,
        limit: str | None = "default",
        **_: Any,
    ) -> Any:
        # expr = self._register_and_transform_cache_tables(expr)
        return super().execute(expr, params=params, limit=limit, **_)

    @staticmethod
    def _find_backend(expr):
        # all dependencies are before the node, that means all the children, but in here the children are like the father
        # so what I want is all the nodes ops.UnboundTable, ops.DatabaseTable, ops.SQLQueryResult that don't have an ancestor
        # that is a CachedNode, that is because the CachedNode changes the backend
        graph, _ = Graph.from_bfs(expr.op(), filter=None, context=None).toposort()

        ancestors = defaultdict(list)

        for node in reversed(graph):
            for child in graph[node]:
                ancestors[child].extend([node, *ancestors.get(node, [])])

        backends = set()
        node_types = (ops.DatabaseTable, ops.SQLQueryResult)
        for table in expr.op().find(node_types):
            if (
                cache := next(
                    (a for a in ancestors[table] if isinstance(a, CachedNode)), None
                )
            ) is not None:
                backends.add(cache.storage.source)
            else:
                backends.add(table.source)

        if len(backends) > 1:
            raise ValueError

        return backends.pop()

    def _into_backend(self, expr: ir.Table, con):
        source = self._find_backend(expr)
        storage = SourceStorage(source=con, key_prefix=self.session_id)
        op = CachedNode(
            schema=expr.schema(),
            parent=expr.op(),
            source=source,
            storage=storage,
        )
        return op.to_expr()
