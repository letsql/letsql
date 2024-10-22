import functools
import itertools
from typing import Any

import ibis
from ibis import Schema, Expr
from ibis.common.collections import FrozenDict
from ibis.expr import operations as ops
from ibis.expr.operations import Relation, Node


def replace_cache_table(node, _, **kwargs):
    if isinstance(node, CachedNode):
        return kwargs["parent"]
    elif isinstance(node, RemoteTable):
        return kwargs["remote_expr"]
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

    def __call__(self, node, _, **kwargs):
        if isinstance(node, Relation):
            updated = {}
            for k, v in list(kwargs.items()):
                try:
                    if v in self.tables:
                        remote: RemoteTableCounter = self.tables[v]
                        name = f"{v.name}_{next(remote.count)}"
                        kwargs[k] = MarkedRemoteTable(
                            name,
                            schema=v.schema,
                            source=v.source,
                            namespace=v.namespace,
                            remote_expr=v.remote_expr,
                        )

                        replacer = RemoteTableReplacer()
                        remote_expr = (
                            remote.remote_expr.op().replace(replacer).to_expr()
                        )
                        batches = remote_expr.to_pyarrow_batches()
                        remote.source.register(batches, table_name=name)
                        updated[v] = kwargs[k]
                        self.created[name] = remote.source
                        self.created.update(replacer.created)

                except TypeError:  # v may not be hashable
                    continue

            if len(updated) > 0:
                kwargs = {k: recursive_update(v, updated) for k, v in kwargs.items()}

        node = node.__recreate__(kwargs)
        if isinstance(node, RemoteTable):
            result = MarkedRemoteTable(
                node.name,
                schema=node.schema,
                source=node.source,
                namespace=node.namespace,
                remote_expr=node.remote_expr,
            )
            self.tables[result] = RemoteTableCounter(node)
            replacer = RemoteTableReplacer()
            remote_expr = node.remote_expr.op().replace(replacer).to_expr()
            batches = remote_expr.to_pyarrow_batches()
            node.source.register(batches, table_name=node.name)
            self.created[node.name] = node.source
            self.created.update(replacer.created)
            node = result

        return node


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

    return node.replace(replace_table).to_expr()


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


class MarkedRemoteTable(ops.DatabaseTable):
    remote_expr: Expr = None


def into_backend(expr, con, name=None):
    return RemoteTable.from_expr(con=con, expr=expr, name=name).to_expr()
