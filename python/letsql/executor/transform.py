import functools
import itertools
from collections import deque, defaultdict
from typing import Any

import dask
import ibis
import pyarrow as pa
import sqlglot.expressions as sge
from ibis.expr import operations as ops
from ibis.expr.operations import DatabaseTable, SQLQueryResult

from letsql.executor.utils import find_backend, SafeTee
from letsql.expr.relations import RemoteTable, CachedNode, Read


def recursive_update(obj, replacements):
    if isinstance(obj, ops.Node):
        if isinstance(obj, RemoteTable):
            return transform(
                obj,
            )
        elif obj in replacements:
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


def _get_batches(schema, batches):
    return pa.RecordBatchReader.from_batches(schema.to_pyarrow(), batches)


class RemoteTableTransformer:
    def __init__(self):
        self.seen = {}  # the keys are the rt and the values are a tuple of batches and counter

    def __call__(self, node: RemoteTable, **kwargs) -> Any:
        from letsql.backends.postgres import Backend as PGBackend

        node = node.__recreate__(kwargs)

        if node in self.seen:
            batches, counter, schema = self.seen[node]
        else:
            schema = node.remote_expr.as_table().schema()
            batches = node.remote_expr.to_pyarrow_batches()
            counter = itertools.count()

        batches, keep = SafeTee.tee(batches, 2)
        batches = _get_batches(schema, batches)
        name = f"clone_{next(counter)}_{node.name}"
        result = DatabaseTable(
            name,
            schema=node.schema,
            source=node.source,
            namespace=node.namespace,
        )
        if isinstance(node.source, PGBackend):
            node.source.read_record_batches(batches, table_name=name)
        else:
            node.source.register(batches, table_name=name)

        self.seen[node] = keep, counter, schema
        return result


@functools.singledispatch
def transform(op, **kwargs):
    return op.__recreate__(kwargs)


transform.register(RemoteTable, RemoteTableTransformer())


@transform.register(CachedNode)
def transform_cached_node(op, schema, parent, source, storage):
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

    return storage.set_default(key, value.op())


@transform.register(Read)
def transform_read(op, **kwargs):
    return op.make_dt()


def sg_replace(sg_expr, replacements):
    for name, tables in replacements.items():
        target = sge.Identifier(this=name, quoted=True)
        for e in (e for e in sg_expr.find_all(sge.Identifier) if e == target):
            new = sge.Identifier(
                this=tables.pop().name,
                quoted=True,
            )
            e.replace(new)


def transform_frame(frame, results=None):
    def replacer(node, _, **kwargs):
        # we replace the op with unbound so we can invoke _to_sqlglot
        if isinstance(node, RemoteTable):
            return ibis.table(node.schema, node.name).op()
        else:
            return node.__recreate__(kwargs)

    nodes = deque(frame[:])
    tables = defaultdict(list)
    while nodes:
        if isinstance(nodes[0], RemoteTable):
            node = nodes.popleft()
            table = transform(node, **dict(zip(node.argnames, node.args)))
            tables[node.name].append(table)
        else:
            break

    if nodes:
        node = nodes[-1]
        expr = node.to_expr()
        backend = expr._find_backend()
        sg_expr = backend.compiler.to_sqlglot(node.replace(replacer).to_expr())
        sg_replace(sg_expr, tables)
        return SQLQueryResult(query=sg_expr.sql(), schema=node.schema, source=backend)
    else:
        return node


def get_val(cache: CachedNode | None) -> ops.Node | None:
    if cache is None:
        return None, None

    expr, storage = cache.parent, cache.storage
    key = storage.get_key(expr)
    if storage.key_exists(key):
        return storage.get(expr)

    return key, None


def transform_plan(plan):
    head, cache, frame = plan

    replacements = None
    key, node = get_val(cache)
    if node is None:
        if head is not None:
            node = transform_plan(head)
            if cache is not None:
                storage = cache.storage
                node = storage.storage._put(key, node)
                replacements = {cache: node}

    if frame:
        node = transform_frame(frame, replacements)

    return node


def get_sql(node):
    def replacer(node, _, **kwargs):
        # we replace the op with unbound so we can invoke _to_sqlglot
        if isinstance(node, RemoteTable):
            return ibis.table(node.schema, node.name).op()
        else:
            return node.__recreate__(kwargs)

    backend = node.to_expr()._find_backend()
    node = node.replace(replacer)
    sg_expr = backend.compiler.to_sqlglot(node.replace(replacer).to_expr())
    return sg_expr, backend
