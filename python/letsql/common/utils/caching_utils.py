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


def find_backend(op: ops.Node, use_default=False) -> tuple[BaseBackend, bool]:
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

    if not backends and use_default:
        from letsql.config import _backend_init

        con = _backend_init()
        backends.add(con)

    return (
        backends.pop(),
        has_unbound,
    )  # TODO what happens if it has more than one backend


def uncached(node):
    return wrap_with_remote_table(node.schema, node.storage.source, node.parent)


def wrap_with_remote_table(schema, target, parent):
    first, _ = find_backend(parent.op())
    if first is not target:
        name = dask.base.tokenize(
            {
                "schema": schema,
                "expr": parent,
                "source": first.name,
                "sink": target.name,
            }
        )
        parent = RemoteTable.from_expr(target, parent, name=name).to_expr()
    return parent
