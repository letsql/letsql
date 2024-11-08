import functools
from typing import Any

import dask
from ibis.expr import operations as ops

from letsql.executor.utils import find_backend
from letsql.expr.relations import RemoteTable, CachedNode, Read


def _recursive_lookup(obj: Any, dct: dict) -> Any:
    """Recursively replace objects in a nested structure with values from a dict.

    Since we treat common collection types inherently traversable, so we need to
    traverse them implicitly and replace the values given a result mapping.

    Parameters
    ----------
    obj
        Object to replace.
    dct
        Mapping of objects to replace with their values.

    Returns
    -------
    Object with replaced values.

    Examples
    --------
    >>> from ibis.common.collections import frozendict
    >>> from ibis.common.grounds import Concrete
    >>> from ibis.common.graph import Node
    >>>
    >>> class MyNode(Concrete, Node):
    ...     number: int
    ...     string: str
    ...     children: tuple[Node, ...]
    >>> a = MyNode(4, "a", ())
    >>>
    >>> b = MyNode(3, "b", ())
    >>> c = MyNode(2, "c", (a, b))
    >>> d = MyNode(1, "d", (c,))
    >>>
    >>> dct = {a: "A", b: "B"}
    >>> _recursive_lookup(a, dct)
    'A'
    >>> _recursive_lookup((a, b), dct)
    ('A', 'B')
    >>> _recursive_lookup({1: a, 2: b}, dct)
    {1: 'A', 2: 'B'}
    >>> _recursive_lookup((a, frozendict({1: c})), dct)
    ('A', {1: MyNode(number=2, ...)})

    """
    if isinstance(obj, ops.Node):
        return dct.get(obj, obj)
    elif isinstance(obj, (tuple, list)):
        return tuple(_recursive_lookup(o, dct) for o in obj)
    elif isinstance(obj, dict):
        return {
            _recursive_lookup(k, dct): _recursive_lookup(v, dct) for k, v in obj.items()
        }
    else:
        return obj


@functools.singledispatch
def transform(op, **kwargs):
    return op.__recreate__(kwargs)


@transform.register(RemoteTable)
def transform_remote_table(op, name, schema, source, namespace, remote_expr):
    kwargs = {
        "name": name,
        "schema": schema,
        "source": source,
        "namespace": namespace,
        "remote_expr": remote_expr,
    }
    node = op.__recreate__(kwargs)
    return node


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


def transform_frame(frame, results=None):
    if results is None:
        results = {}

    for node in frame:
        kwargs = {
            k: _recursive_lookup(v, results)
            for k, v in zip(node.__argnames__, node.__args__)
        }
        results[node] = transform(node, **kwargs)

    return results[frame[-1]]


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
