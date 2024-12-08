import dask.base

from ibis import BaseBackend
from letsql.expr.relations import CachedNode, RemoteTable, Read
from ibis.expr import operations as ops


def transform_cached_node(op, _, **kwargs):
    op = op.__recreate__(kwargs)

    if isinstance(op, CachedNode):
        uncached, storage = op.parent, op.storage

        first, _ = find_backend(uncached.op())
        other = storage.source

        if first is not other:
            name = dask.base.tokenize(
                {
                    "schema": op.schema,
                    "expr": uncached.unbind(),
                    "source": first.name,
                    "sink": other.name,
                }
            )

            parent = RemoteTable.from_expr(other, uncached, name=name).to_expr()
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


def exists(node: CachedNode):
    parent = uncached(node)
    key = node.storage.get_key(parent)
    return node.storage.key_exists(key)


def uncached(node):
    first, _ = find_backend(node.parent.op())
    other = node.storage.source
    parent = node.parent
    if first is not other:
        name = dask.base.tokenize(
            {
                "schema": node.schema,
                "expr": parent.unbind(),
                "source": first.name,
                "sink": other.name,
            }
        )
        parent = RemoteTable.from_expr(other, parent, name=name).to_expr()
    return parent
