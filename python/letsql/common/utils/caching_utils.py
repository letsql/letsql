import dask.base

from ibis import BaseBackend
from letsql.expr.relations import CachedNode, RemoteTable, Read
from ibis.expr import operations as ops


def transform_cached_node(op, _, **kwargs):
    op = op.__recreate__(kwargs)

    if isinstance(op, CachedNode):
        storage = op.storage
        parent = uncached(op)

        op = CachedNode(
            schema=op.schema,
            parent=parent,
            source=storage.source,
            storage=storage,
        )

    return op


def find_backend(op: ops.Node) -> tuple[BaseBackend, bool]:
    backends = set()
    has_unbound = False
    node_types = (
        ops.UnboundTable,
        ops.DatabaseTable,
        ops.SQLQueryResult,
        CachedNode,
        Read,
    )
    for table in op.find(node_types):
        if isinstance(table, ops.UnboundTable):
            has_unbound = True
        else:
            backends.add(table.source)

    return (
        backends.pop(),
        has_unbound,
    )  # TODO what happens if it has more than one backend


def uncached(node):
    parent = node.parent
    first, _ = find_backend(parent.op())
    other = node.storage.source

    if first is not other:
        name = dask.base.tokenize(
            {
                "schema": node.schema,
                "expr": parent,
                "source": first.name,
                "sink": other.name,
            }
        )
        parent = RemoteTable.from_expr(other, parent, name=name).to_expr()
    return parent
