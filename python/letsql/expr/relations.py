import functools
import itertools
from typing import Any

import ibis
from ibis import Schema, Expr
from ibis.common.collections import FrozenDict
from ibis.expr import operations as ops
from ibis.expr.operations import DatabaseTable

import letsql as ls


def replace_cache_table(node, _, **kwargs):
    if isinstance(node, CachedNode):
        return kwargs["parent"]
    elif isinstance(node, RemoteTable):
        return kwargs["remote_expr"]
    else:
        return node.__recreate__(kwargs)


class RemoteTableReplacer:
    def __init__(self):
        self.tables = {}
        self.count = itertools.count()

    def __call__(self, node, _, **kwargs):
        for k, v in list(kwargs.items()):
            try:
                if v in self.tables:
                    name = f"{v.name}_{next(self.count)}"
                    kwargs[k] = DatabaseTable(
                        name,
                        schema=v.schema,
                        source=v.source,
                        namespace=v.namespace,
                    )
                    remote: RemoteTable = self.tables[v]
                    batches = remote.remote_expr.to_pyarrow_batches()
                    remote.source.register(batches, table_name=name)

            except TypeError:  # v may not be hashable
                continue

        node = node.__recreate__(kwargs)
        if isinstance(node, RemoteTable):
            result = DatabaseTable(
                node.name,
                schema=node.schema,
                source=node.source,
                namespace=node.namespace,
            )
            self.tables[result] = node
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
    remote_expr: Expr

    @classmethod
    def from_expr(cls, con, expr, name=None):
        name = name or gen_name()
        return cls(
            name=name,
            schema=expr.schema(),
            source=con,
            remote_expr=expr,
        )

    @classmethod
    def from_rbr(cls, con, rbr, name=None):
        expr = ls.connect().register(rbr, gen_name())
        return cls.from_expr(con, expr, name)


def into_backend(expr, con, name=None):
    return RemoteTable.from_expr(con=con, expr=expr, name=name).to_expr()
