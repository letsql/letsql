import functools

from ibis import BaseBackend
from ibis.expr import operations as ops
from ibis.expr import types as ir

from letsql.expr.relations import RemoteTable, CachedNode, RemoteTableReplacer


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


def find_backend(op: ops.Node) -> tuple[BaseBackend, bool]:
    backends = set()
    has_unbound = False
    node_types = (ops.UnboundTable, ops.DatabaseTable, ops.SQLQueryResult)
    for table in op.find(node_types):
        if isinstance(table, ops.UnboundTable):
            has_unbound = True
        else:
            backends.add(table.source)

    return (
        backends.pop(),
        has_unbound,
    )  # TODO what happens if it has more than one backend


@functools.singledispatch
def collect_(op, **kwargs):
    return op.__recreate__(kwargs)


@collect_.register(ops.Relation)
def _relation(op, parent=None, **kwargs):
    kwargs["parent"] = parent
    return op.__recreate__(kwargs)


@collect_.register(ops.PhysicalTable)
def _physical_table(op, name, **kwargs):
    return op.__recreate__({"name": name, **kwargs})


@collect_.register(ops.UnboundTable)
@collect_.register(ops.DatabaseTable)
def _unbound_table(op, name, **kwargs):
    return op.__recreate__({"name": name, **kwargs})


@collect_.register(ops.InMemoryTable)
def _in_memory_table(op, data, **kwargs):
    return op.__recreate__({"data": data, **kwargs})


@collect_.register(ops.SQLQueryResult)
@collect_.register(ops.SQLStringView)
def _sql_query_result(op, query, **kwargs):
    kwargs["query"] = query
    return op.__recreate__(kwargs)


@collect_.register(ops.Aggregate)
def _aggregate(op, parent, **kwargs):
    kwargs["parent"] = parent
    return op.__recreate__(kwargs)


@collect_.register(ops.Project)
def _project(op, parent, values):
    return op.__recreate__({"parent": parent, "values": values})


@collect_.register(ops.DummyTable)
def _dummy_table(op, values):
    return op.__recreate__({"values": values})


@collect_.register(ops.Filter)
def _project(op, parent, predicates):
    return op.__recreate__({"parent": parent, "predicates": predicates})


@collect_.register(ops.Sort)
def _sort(op, parent, keys):
    return op.__recreate__({"parent": parent, "keys": keys})


@collect_.register(ops.Set)
def _set_op(op, left, right, distinct):
    kwargs = {"left": left, "right": right, "distinct": distinct}
    source, _ = find_backend(left)
    candidate, _ = find_backend(right)

    updated = {}
    if source is not candidate:
        expr = right.to_expr()
        table = RemoteTable.from_expr(source, expr)
        updated[right] = table

    if updated:
        kwargs = {k: recursive_update(v, updated) for k, v in kwargs.items()}

    return op.__recreate__(kwargs)


@collect_.register(ops.JoinLink)
def _join(op, how, table, predicates):
    return op.__recreate__({"how": how, "table": table, "predicates": predicates})


@collect_.register(ops.Limit)
@collect_.register(ops.Sample)
def _limit(op, parent, **kwargs):
    return op.__recreate__({"parent": parent, **kwargs})


@collect_.register(ops.SelfReference)
@collect_.register(ops.Distinct)
def _self_reference(op, parent, **kwargs):
    return op.__recreate__({"parent": parent, **kwargs})


@collect_.register(ops.JoinReference)
def _join_reference(op, parent, **kwargs):
    return op.__recreate__({"parent": parent, **kwargs})


@collect_.register(ops.Literal)
def _literal(op, value, **kwargs):
    return op.__recreate__({"value": value, **kwargs})


@collect_.register(ops.Field)
def _relation_field(op, rel, name):
    return op.__recreate__({"rel": rel, "name": name})


@collect_.register(ops.Value)
def _value(op, **kwargs):
    return op.__recreate__(kwargs)


@collect_.register(ops.Alias)
def _alias(op, arg, name):
    return op.__recreate__({"arg": arg, "name": name})


@collect_.register(ops.Binary)
def _binary(op, left, right):
    return op.__recreate__({"left": left, "right": right})


@collect_.register(ops.ScalarParameter)
def _scalar_parameter(op, dtype, **kwargs):
    return op.__recreate__({"dtype": dtype, **kwargs})


@collect_.register(ops.SortKey)
def _sort_key(op, expr, **kwargs):
    return op.__recreate__({"expr": expr, **kwargs})


@collect_.register(ops.JoinChain)
def _join_project(op, first, rest, values):
    kwargs = {"first": first, "rest": rest, "values": values}
    source, _ = find_backend(first)

    updated = {}
    for opi in rest:
        expr = opi.table.to_expr()
        candidate, _ = find_backend(opi.table)
        if candidate is not source:
            table = RemoteTable.from_expr(source, expr)
            reference = ops.JoinReference(table, opi.table.identifier)
            updated[opi.table] = reference

    if updated:
        kwargs = {k: recursive_update(v, updated) for k, v in kwargs.items()}

    return op.__recreate__(kwargs)


@collect_.register(CachedNode)
def _cached_node(op, schema, parent, source, storage):
    first, _ = find_backend(parent)
    other = storage.source

    if first is not other:
        # TODO create the remote table if other can register RecordBatchReader
        # TODO do a per backend analysis of what to do when resolving a RemoteTable
        table = RemoteTable.from_expr(other, parent.to_expr())
        return CachedNode(schema=schema, parent=table, source=other, storage=storage)
    else:
        kwargs = {
            "schema": schema,
            "parent": parent,
            "source": source,
            "storage": storage,
        }
        return op.__recreate__(kwargs)


# TODO keep track of every table created
# TODO implement recursive collect for into_backend
# TODO optimize to make the minimum data movement when there are more than 2 available backend
# TODO using an identifier won't work the solution is to use a _register_remote_tables before caching and
# TODO _register_and_transform_remote_tables after caching
# TODO like the following
# TODO _register_and_transform_cache_tables (with _register_remote_tables)
# TODO _register_and_transform_remote_tables
# TODO create dispatch based for registering batches


def _register_and_transform_remote_tables(node):
    replacer = RemoteTableReplacer()
    return node.replace(replacer), replacer.created


def _register_and_transform_cache_tables(op):
    """This function will sequentially execute any cache node that is not already cached"""

    created = {}

    def fn(node, _, **kwargs):
        node = node.__recreate__(kwargs)
        if isinstance(node, CachedNode):
            uncached, storage = node.parent, node.storage

            replacer = RemoteTableReplacer()
            uncached_value = uncached.replace(replacer)
            created.update(replacer.created)

            node = storage.set_default(uncached.to_expr(), uncached_value)
        return node

    out = op.replace(fn)

    for table, con in created.items():
        try:
            con.drop_table(table)
        except Exception:
            try:
                con.drop_view(table)
            except Exception:
                pass

    return out


def execute(expr: ir.Expr):
    def evaluator(op, _, **kwargs):
        return collect_(op, **kwargs)

    node = expr.op()
    results = node.map(
        evaluator
    )  # TODO Can we build a new graph from results, if needed?
    node = results[node]
    node = _register_and_transform_cache_tables(node)
    node, _ = _register_and_transform_remote_tables(node)
    expr = node.to_expr()

    return expr.execute()


def to_pyarrow_batches(expr: ir.Expr):
    def evaluator(op, _, **kwargs):
        return collect_(op, **kwargs)

    node = expr.op()
    results = node.map(
        evaluator
    )  # TODO Can we build a new graph from results, if needed?
    node = results[node]
    node = _register_and_transform_cache_tables(node)
    node, _ = _register_and_transform_remote_tables(node)
    expr = node.to_expr()

    return expr.to_pyarrow_batches()


def to_pyarrow(expr: ir.Expr):
    def evaluator(op, _, **kwargs):
        return collect_(op, **kwargs)

    node = expr.op()
    results = node.map(
        evaluator
    )  # TODO Can we build a new graph from results, if needed?
    node = results[node]
    node = _register_and_transform_cache_tables(node)
    node, _ = _register_and_transform_remote_tables(node)
    expr = node.to_expr()

    return expr.to_pyarrow()
