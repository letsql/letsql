from xorq.expr.relations import CachedNode, Read
from xorq.vendor.ibis import BaseBackend
from xorq.vendor.ibis.expr import operations as ops


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
        from xorq.config import _backend_init

        con = _backend_init()
        backends.add(con)

    return (
        backends.pop(),
        has_unbound,
    )  # TODO what happens if it has more than one backend
