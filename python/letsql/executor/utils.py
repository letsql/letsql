import dask.base

from ibis import BaseBackend
from letsql.expr.relations import CachedNode, RemoteTable, Read
from ibis.expr import operations as ops


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
                "expr": parent,
                "source": first.name,
                "sink": other.name,
            }
        )
        parent = RemoteTable.from_expr(other, parent, name=name).to_expr()
    return parent


# https://stackoverflow.com/questions/6703594/is-the-result-of-itertools-tee-thread-safe-python
class SafeTee(object):
    """tee object wrapped to make it thread-safe"""

    def __init__(self, teeobj, lock):
        self.teeobj = teeobj
        self.lock = lock

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.teeobj)

    def __copy__(self):
        return SafeTee(self.teeobj.__copy__(), self.lock)

    @classmethod
    def tee(cls, iterable, n=2):
        """tuple of n independent thread-safe iterators"""
        from itertools import tee
        from threading import Lock

        lock = Lock()
        return tuple(cls(teeobj, lock) for teeobj in tee(iterable, n))
