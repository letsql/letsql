import functools
from operator import methodcaller
from pathlib import Path
from typing import Any

import dask.base
from ibis.expr import operations as ops
from ibis.expr import types as ir

from letsql.executor.collected import Collected, RemoteTableCollected
from letsql.executor.utils import find_backend
from letsql.expr.relations import RemoteTable, CachedNode, Read


def recursive_update(obj, replacements):
    if isinstance(obj, ops.Node):
        if obj in replacements:
            return replacements[obj]
        else:
            return obj.__recreate__(
                {
                    name: recursive_update(arg, replacements)
                    for name, arg in zip(obj.argnames, obj.args)
                }
            )
    elif isinstance(obj, (tuple, list)):
        return tuple(recursive_update(o, replacements) for o in obj)
    elif isinstance(obj, dict):
        return {
            recursive_update(k, replacements): recursive_update(v, replacements)
            for k, v in obj.items()
        }
    else:
        return obj


class Collector:
    def __init__(self, relation=False):
        self.updated = {}
        self.relation = relation

    def __call__(self, obj):
        if isinstance(obj, Collected):
            val = obj.value()
            if isinstance(obj, RemoteTableCollected) and self.relation:
                self.updated[obj.node] = val
            else:
                val = obj.node
            return val
        elif isinstance(obj, (tuple, list)):
            return tuple(self(o) for o in obj)
        elif isinstance(obj, dict):
            return {self(k): self(v) for k, v in obj.items()}
        else:
            return obj


@functools.singledispatch
def transform_execute(op, **kwargs):
    collector = Collector(relation=isinstance(op, ops.Relation))
    kwargs = collector(kwargs)  # this set all normal kwargs
    if len(collector.updated) > 0:
        kwargs = {k: recursive_update(v, collector.updated) for k, v in kwargs.items()}

    node = op.__recreate__(kwargs)
    return Collected(node)


@transform_execute.register(RemoteTable)
def transform_execute_remote_table(op, name, schema, source, namespace, remote_expr):
    kwargs = {
        "name": name,
        "schema": schema,
        "source": source,
        "namespace": namespace,
        "remote_expr": remote_expr,
    }
    node = op.__recreate__(kwargs)
    return RemoteTableCollected(node)


@transform_execute.register(CachedNode)
def transform_execute_cached_node(op, schema, parent, source, storage):
    first, _ = find_backend(parent.op())
    other = storage.source

    key = op.parent
    value = parent
    if first is not other:
        value = RemoteTable.from_expr(other, value).to_expr()

        name = dask.base.tokenize(
            {
                "schema": schema,
                "expr": key.unbind(),
                "source": first.name,
                "sink": other.name,
            }
        )
        key = RemoteTable.from_expr(other, key, name=name).to_expr()

    return Collected(storage.set_default(key, value.op()))


@transform_execute.register(Read)
def transform_execute_read(op, **_kwargs):
    return Collected(op.make_dt())


def _template(expr: ir.Expr, method_name: str, **kwargs) -> ir.Expr:
    function = methodcaller(method_name, **kwargs)

    if not expr.op().find((RemoteTable, CachedNode, Read)):  # is a single engine expr
        return function(expr)
    else:

        def evaluator(op, _, **_kwargs):
            return transform_execute(op, **_kwargs)

        node = expr.op()
        results = node.map(
            evaluator
        )  # TODO Can we build a new graph from results, if needed?
        node = results[node].value()

        assert not node.find((RemoteTable, CachedNode, Read))
        return function(node.to_expr())


def execute(expr: ir.Expr):
    return _template(expr, "execute")


def to_pyarrow_batches(expr: ir.Expr, **kwargs) -> ir.Expr:
    return _template(expr, "to_pyarrow_batches", **kwargs)


def to_pyarrow(expr: ir.Expr):
    return _template(expr, "to_pyarrow")


def to_parquet(expr: ir.Expr, path: str | Path, **kwargs: Any):
    if not expr.op().find((RemoteTable, CachedNode, Read)):
        expr.to_parquet(path, **kwargs)
    else:

        def evaluator(op, _, **_kwargs):
            return transform_execute(op, **_kwargs)

        node = expr.op()
        results = node.map(evaluator)
        node = results[node].value()

        assert not node.find((RemoteTable, CachedNode, Read))
        node.to_expr().to_parquet(path, **kwargs)
