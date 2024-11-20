import functools
import itertools
from typing import Any

import ibis
import pyarrow as pa
from ibis import Schema, Expr
from ibis.common.collections import FrozenDict
from ibis.expr import operations as ops
from ibis.expr.operations import Relation, Node, DatabaseTable

from letsql.common.utils.graph_utils import replace_fix, get_args


def replace_cache_table(node, _, **kwargs):
    if isinstance(node, CachedNode):
        return kwargs["parent"].op().replace(replace_fix(replace_cache_table))
    elif isinstance(node, RemoteTable):
        return kwargs["remote_expr"].op()
    else:
        return node.__recreate__(kwargs)


class RemoteTableCounter:
    def __init__(self, table):
        self.table = table
        self.count = itertools.count()

    @property
    def remote_expr(self):
        return self.table.remote_expr

    @property
    def source(self):
        return self.table.source


def recursive_update(obj, replacements):
    if isinstance(obj, Node):
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


class RemoteTableReplacer:
    def __init__(self):
        self.tables = {}
        self.count = itertools.count()
        self.created = {}
        self.seen_expr = {}

    def __call__(self, *args, **kwargs):
        from letsql.backends.postgres import Backend as PGBackend

        node, _, kwargs = get_args(*args, **kwargs)
        if isinstance(node, Relation):
            updated = {}
            for k, v in list(kwargs.items()):
                try:
                    if v in self.tables:
                        remote: RemoteTableCounter = self.tables[v]
                        name = f"pre_{next(remote.count)}_{v.name}"
                        kwargs[k] = DatabaseTable(
                            name,
                            schema=v.schema,
                            source=v.source,
                            namespace=v.namespace,
                        )

                        batches = self.get_batches(remote.remote_expr)
                        if isinstance(remote.source, PGBackend):
                            remote.source.read_record_batches(batches, table_name=name)
                        else:
                            remote.source.register(batches, table_name=name)
                        updated[v] = kwargs[k]
                        self.created[name] = remote.source

                except TypeError:  # v may not be hashable
                    continue

            if len(updated) > 0:
                kwargs = {k: recursive_update(v, updated) for k, v in kwargs.items()}

        node = node.__recreate__(kwargs)
        if isinstance(node, RemoteTable):
            result = DatabaseTable(
                node.name,
                schema=node.schema,
                source=node.source,
                namespace=node.namespace,
            )
            self.tables[result] = RemoteTableCounter(node)
            batches = self.get_batches(node.remote_expr)
            if isinstance(node.source, PGBackend):
                node.source.read_record_batches(batches, table_name=node.name)
            else:
                node.source.register(batches, table_name=node.name)
            self.created[node.name] = node.source
            node = result

        return node

    def get_batches(self, expr):
        from letsql.executor import to_pyarrow_batches
        from letsql.executor.utils import SafeTee

        def finder(op):
            return isinstance(op, RemoteTable) or op in self.tables

        if not expr.op().find(finder):
            if expr not in self.seen_expr:
                batches = to_pyarrow_batches(expr)
                result, keep = SafeTee.tee(batches, 2)
                self.seen_expr[expr] = keep
            else:
                result, keep = SafeTee.tee(self.seen_expr[expr], 2)
                self.seen_expr[expr] = keep
        elif expr not in self.seen_expr:
            batches = to_pyarrow_batches(expr)
            result, keep = SafeTee.tee(batches, 2)
            self.seen_expr[expr] = keep
        else:
            result, keep = SafeTee.tee(self.seen_expr[expr], 2)
            self.seen_expr[expr] = keep

        schema = expr.as_table().schema()
        return pa.RecordBatchReader.from_batches(schema.to_pyarrow(), result)


def replace_source_factory(source: Any):
    def replace_source(node, _, **kwargs):
        if "source" in kwargs:
            kwargs["source"] = source
        return node.__recreate__(kwargs)

    return replace_source


def make_native_op(node):
    # FIXME: how to reference let.Backend.name?
    if node.source.name != "let":
        raise ValueError
    sources = node.source._sources
    native_source = sources.get_backend(node)
    if native_source.name == "let":
        raise ValueError

    def replace_table(_node, _, **_kwargs):
        return sources.get_table_or_op(_node, _node.__recreate__(_kwargs))

    return node.replace(replace_fix(replace_table)).to_expr()


class CachedNode(ops.Relation):
    schema: Schema
    parent: Any
    source: Any
    storage: Any
    values = FrozenDict()


gen_name = functools.partial(ibis.util.gen_name, "remote-expr-placeholder")


class RemoteTable(ops.DatabaseTable):
    remote_expr: Expr = None

    @classmethod
    def from_expr(cls, con, expr, name=None):
        name = name or gen_name()
        return cls(
            name=name,
            schema=expr.schema(),
            source=con,
            remote_expr=expr,
        )


def into_backend(expr, con, name=None):
    return RemoteTable.from_expr(con=con, expr=expr, name=name).to_expr()


class Read(ops.Relation):
    method_name: str
    name: str
    schema: Schema
    source: Any
    read_kwargs: Any
    values = FrozenDict()

    def make_dt(self):
        method = getattr(self.source, self.method_name)
        dt = method(**dict(self.read_kwargs)).op()
        return dt

    def make_unbound_dt(self):
        import dask

        name = f"{self.name}-{dask.base.tokenize(self)}"
        return ops.UnboundTable(
            name=name,
            schema=self.schema,
        )
