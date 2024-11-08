from collections import deque
from itertools import chain

import dask
from ibis.expr import operations as ops

from letsql.executor.utils import find_backend
from letsql.expr.relations import RemoteTable, CachedNode
from ibis.expr import types as ir


def find(obj):
    if isinstance(obj, RemoteTable):
        yield obj
    elif isinstance(obj, (tuple, list)):
        for ox in obj:
            if isinstance(ox, RemoteTable) or ():
                yield ox
            elif isinstance(ox, ir.Expr) and isinstance(ox.op(), RemoteTable):
                yield ox.op()
    elif isinstance(obj, dict):
        for ox in obj.values():
            if isinstance(ox, RemoteTable):
                yield ox
            elif isinstance(ox, ir.Expr) and isinstance(ox.op(), RemoteTable):
                yield ox.op()


def replace_cross_source_cached_nodes(node: ops.Node):
    def fn(op, _, **kwargs):
        op = op.__recreate__(kwargs)
        if isinstance(op, CachedNode):
            storage, parent = op.storage, op.parent
            first, _ = find_backend(parent.op())
            other = storage.source

            if first is not other:
                name = dask.base.tokenize(
                    {
                        "schema": op.schema,
                        "expr": parent.unbind(),
                        "source": first.name,
                        "sink": other.name,
                    }
                )
                kwargs["parent"] = RemoteTable.from_expr(
                    other, parent, name=name
                ).to_expr()
                return op.__recreate__(kwargs)
        return op

    return node.replace(fn)


def nest(lst):
    head, cache = None, None
    for i, value in enumerate(reversed(lst), 1):
        if isinstance(value, CachedNode):
            head = nest(lst[: len(lst) - i])
            return [head, value, lst[len(lst) - i + 1 :]]

    return [head, cache, lst]


def segment(node: ops.Node):
    graph = make_graph(node)

    dependents = set(chain.from_iterable(graph.values()))
    node = next((no for no in graph if no not in dependents), None)
    assert node is not None

    result = deque([node])
    stack = deque([node])

    while stack:
        no = stack.popleft()

        if no in graph:
            for child in graph[no]:
                stack.appendleft(child)
                if isinstance(child, (RemoteTable, CachedNode)) or (
                    (no in result) and isinstance(no, CachedNode)
                ):
                    result.appendleft(child)

    return nest(list(result))


def make_graph(node: ops.Node):
    node = replace_cross_source_cached_nodes(node)

    dag = {}

    if isinstance(node, RemoteTable):
        remote_expr_node = node.remote_expr.op()
        dag[node] = [remote_expr_node]
        dag.update(make_graph(remote_expr_node))
    elif isinstance(node, CachedNode):
        cached_expr_node = node.parent.op()
        dag[node] = [cached_expr_node]
        dag.update(make_graph(cached_expr_node))

    found = []

    def evaluator(op, _, **_kwargs):
        if isinstance(op, ops.Relation):
            for no in find(_kwargs):
                found.append(no)

        return op

    node.map(evaluator)

    if found:
        dag[node] = found
        for table in found:
            remote_expr_op = table.remote_expr.op()
            dag[table] = [remote_expr_op]
            dag.update(make_graph(remote_expr_op))

    return dag


def execute(expr: ir.Expr):
    head, cache, tail = segment(expr.op())

    if cache is not None:
        # eval the head
        # cache
        # eval the tail
        pass
