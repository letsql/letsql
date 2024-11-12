from collections import defaultdict, Counter
from itertools import chain
from operator import methodcaller

import ibis
import sqlglot.expressions as sge
from ibis.expr import operations as ops
from ibis.expr import types as ir
from ibis.expr.operations import SQLQueryResult

from letsql.executor.utils import find_backend, SafeTee
from letsql.expr.relations import RemoteTable, CachedNode
import pyarrow as pa


def _make_graph(node: ops.Node) -> dict[ops.Node, list[ops.Node]]:
    dag = {}
    children = list(node.find(RemoteTable))

    dag[node] = children
    for child in children:
        nested = child.remote_expr.op()
        if nested.find(RemoteTable):
            dag.update(_make_graph(nested))

    return dag


def _get_root(graph: dict[ops.Node, list[ops.Node]]) -> ops.Node:
    candidates = set(graph.keys()) - set(chain.from_iterable(graph.values()))
    assert len(candidates) == 1
    return candidates.pop()


class Segment:
    def __init__(self, node: ops.Node, name=None) -> None:
        self.__id = None
        self.node = node
        self.remote_tables = list(
            node.find(RemoteTable)
        )  # I think this misses remote tables in join

        children = []
        for child in self.remote_tables:
            nested = child.remote_expr.op()
            if nested.find(RemoteTable):
                children.append(Segment(nested, name=nested.name))
        self.name = name
        self.dependencies = children
        self._is_leaf = None
        self._backend = None
        self._query = None
        self.keep = None

    @property
    def _id(self):
        import uuid
        import random

        if self.__id is None:
            rid = uuid.uuid4().hex
            self.__id = random.sample(rid, k=len(rid))[:5]

        return self.__id

    @classmethod
    def from_expr(cls, expr: ir.Expr):
        return cls(expr.op())

    def find_topmost_not_empy_cache(self):
        pass

    def find_bottommost_empty_cache(self):
        pass

    @property
    def is_leaf(self):
        if self._is_leaf is None:
            self._is_leaf = not bool(self.dependencies)
        return self._is_leaf

    def to_s(self, level: int = 0) -> str:
        indent = "  " * level

        backend, query = self.backend, str(self.query.sql())
        lines = [
            f"{indent}backend := {backend.name}\n{indent}name := {self.name or ''}\n{indent}query := {query}"
        ]

        if self.is_leaf:
            lines.append("--------")
            indent = "  " * (level + 2)
            for remote_table in self.remote_tables:
                backend, _ = find_backend(remote_table.remote_expr.op())
                name = remote_table.name
                query = backend.compiler.to_sqlglot(remote_table.remote_expr).sql()
                lines.append(
                    f"{indent}backend := {backend.name}\n{indent}name := {name}\n{indent}query := {query}"
                )
        else:
            for dependency in self.dependencies:
                lines.append(dependency.to_s(level=level + 2))

        return "\n".join(lines)

    @property
    def schema(self):
        return self.node.schema

    @property
    def backend(self):
        if self._backend is None:
            self._backend, _ = find_backend(self.node)
        return self._backend

    @property
    def query(self):
        def replacer(node, _, **kwargs):
            if isinstance(node, CachedNode):
                return kwargs["parent"].op().replace(replacer)
            if isinstance(
                node, RemoteTable
            ):  # only care about one-level deep RemoteTable since a segment only contains those types
                return ibis.table(node.schema, node.name).op()
            else:
                return node.__recreate__(kwargs)

        return self.backend.compiler.to_sqlglot(self.node.replace(replacer).to_expr())

    def __repr__(self):
        return self.to_s()

    def is_cached(self):
        return self.node.find(CachedNode)

    def cache_exists(self):
        from letsql.executor.utils import exists as ex

        def _exists(node):
            cached = node.find(CachedNode)
            if not cached:
                yield None
            else:
                for no in cached:
                    yield ex(no)
                    yield from _exists(no.parent.op())

        if self.is_cached:
            # must iterate from the bottom up else we execute downstream cached tables
            return all(val for val in _exists(self.node) if val is not None)
        else:
            return False

    def _count_tables(self):
        def names():
            query = self.query
            # FIXME What happens if you have duplicated table names
            for table in self.remote_tables:
                name = table.name
                target = sge.Identifier(this=name, quoted=True)
                for _ in (e for e in query.find_all(sge.Identifier) if e == target):
                    yield name

        return Counter(names())

    def _replace_sqlglot(self, query, replacements):
        # FIXME What happens if you have duplicated table names
        for old, news in replacements:
            target = sge.Identifier(this=old, quoted=True)
            for e in (e for e in query.find_all(sge.Identifier) if e == target):
                new = sge.Identifier(
                    this=news.pop(),
                    quoted=True,
                )
                e.replace(new)
        return query

    def register_remote_tables(self):
        # use SQL to get the actual number of remote tables to register
        # we have to use to_pyarrow_batches in case there is a cache in one of the RT
        tables_count = self._count_tables()

        remote_tables = defaultdict(list)
        for dependency in self.dependencies:
            count = tables_count[dependency.name]
            for i in range(count):
                name = f"clone_{self._id}_{i}_{dependency.name}"
                self._register_remote_table(name, dependency._to_pyarrow_batches())
                remote_tables[dependency.name].append(name)

        return remote_tables

    def _register_remote_table(self, name, batches):
        from letsql.backends.postgres import Backend as PGBackend

        method_name = (
            "read_record_batches" if isinstance(self.backend, PGBackend) else "register"
        )
        method = methodcaller(method_name, batches, table_name=name)

        return method

    def transform(self) -> ops.Node:
        replacements = {}
        if not self.is_cached:
            replacements = self.register_remote_tables()

        def fn(node, _, **kwargs):
            node = node.__recreate__(kwargs)
            if isinstance(node, CachedNode):
                uncached, storage = node.parent, node.storage
                node = storage.set_default(uncached, uncached.op())
            if isinstance(
                node, RemoteTable
            ):  # only care about one-level deep RemoteTable since a segment only contains those types
                return ibis.table(node.schema, node.name).op()
            return node

        op = self.node.replace(fn)
        query = self.backend.compiler.to_sqlglot(op.to_expr())
        query = self._replace_sqlglot(query, replacements)

        return SQLQueryResult(query.sql(), self.node.schema, self.backend)

    def _get_batches(self, batches):
        schema = self.node.schema()
        return pa.RecordBatchReader.from_batches(schema.to_pyarrow(), batches)

    def _to_pyarrow_batches(self):
        if not self.is_cached:
            self.register_remote_tables()
        expr = self.transform().to_expr()

        if self.keep is None:
            self.keep = expr.to_pyarrow_batches()

        batches, keep = SafeTee.tee(self.keep, 2)
        batches = self._get_batches(batches)
        self.keep = keep

        return batches


def execute(expr: ir.Expr):
    segment = Segment.from_expr(expr)
    expr = segment.transform().to_expr()
    return expr.execute()
