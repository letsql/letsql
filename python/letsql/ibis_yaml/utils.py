import base64
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Tuple

import cloudpickle

import letsql.vendor.ibis.expr.operations as ops
import letsql.vendor.ibis.expr.types as ir
from letsql.common.caching import SourceStorage
from letsql.expr.relations import CachedNode, Read, RemoteTable
from letsql.vendor.ibis.backends import BaseBackend
from letsql.vendor.ibis.common.collections import FrozenOrderedDict
from letsql.vendor.ibis.expr.types.relations import Table


def serialize_udf_function(fn: callable) -> str:
    pickled = cloudpickle.dumps(fn)
    encoded = base64.b64encode(pickled).decode("ascii")
    return encoded


def deserialize_udf_function(encoded_fn: str) -> callable:
    pickled = base64.b64decode(encoded_fn)
    return cloudpickle.loads(pickled)


def freeze(obj):
    if isinstance(obj, dict):
        return FrozenOrderedDict({k: freeze(v) for k, v in obj.items()})
    elif isinstance(obj, list):
        return tuple(freeze(x) for x in obj)
    return obj


class MissingValue:
    def __repr__(self):
        return "<MISSING>"


MISSING = MissingValue()


def deep_diff_objects(obj1, obj2, path="root"):
    differences = []

    if obj1 is not obj2:
        differences.append((path, obj1, obj2))
        return differences

    if isinstance(obj1, Mapping):
        keys1 = set(obj1.keys())
        keys2 = set(obj2.keys())
        for key in keys1 - keys2:
            diff_path = f"{path}.{key}" if path else key
            differences.append((diff_path, obj1[key], MISSING))
        for key in keys2 - keys1:
            diff_path = f"{path}.{key}" if path else key
            differences.append((diff_path, MISSING, obj2[key]))
        for key in keys1 & keys2:
            diff_path = f"{path}.{key}" if path else key
            differences.extend(deep_diff_objects(obj1[key], obj2[key], diff_path))
        return differences

    elif isinstance(obj1, Sequence) and not isinstance(obj1, str):
        if len(obj1) != len(obj2):
            differences.append((path, obj1, obj2))
        for i, (item1, item2) in enumerate(zip(obj1, obj2)):
            diff_path = f"{path}[{i}]"
            differences.extend(deep_diff_objects(item1, item2, diff_path))
        return differences

    else:
        if obj1 != obj2:
            differences.append((path, obj1, obj2))
        return differences


def serialize_ibis_expr(expr):
    try:
        op = expr.op()
    except Exception:
        return repr(expr)

    serialized = {
        "expr_class": expr.__class__.__name__,
        "op_class": op.__class__.__name__,
    }

    op_attrs = {}
    for attr in dir(op):
        if attr.startswith("_"):
            continue
        try:
            value = getattr(op, attr)
        except Exception:
            continue
        if callable(value):
            continue
        op_attrs[attr] = value
    if op_attrs:
        serialized["op_attrs"] = op_attrs

    if hasattr(op, "args"):
        try:
            children = op.args
        except Exception:
            children = None
        if children is not None:
            if isinstance(children, Sequence) and not isinstance(children, str):
                serialized["args"] = [serialize_ibis_expr(child) for child in children]
            else:
                serialized["args"] = serialize_ibis_expr(children)
    return serialized


def diff_ibis_exprs(expr1, expr2):
    if expr1.equals(expr2):
        print("Expressions are equal")
        return

    serialized1 = serialize_ibis_expr(expr1)
    serialized2 = serialize_ibis_expr(expr2)

    diffs = deep_diff_objects(serialized1, serialized2)
    if diffs:
        print("Found differences:")
        for diff in diffs:
            path, val1, val2 = diff
            print(f"At {path}:")
            print(f"  First expression: {val1}")
            print(f"  Second expression: {val2}")
    else:
        print("No differences found (unexpectedly).")

    return diffs


def translate_storage(storage, compiler: Any) -> Dict:
    if isinstance(storage, SourceStorage):
        return {
            "type": "SourceStorage",
            "source": storage.source._profile.hash_name,
        }
    else:
        raise NotImplementedError(f"Unknown storage type: {type(storage)}")


def load_storage_from_yaml(storage_yaml: Dict, compiler: Any):
    if storage_yaml["type"] == "SourceStorage":
        source_profile_name = storage_yaml["source"]
        source = compiler.profiles[source_profile_name]
        return SourceStorage(source=source)
    else:
        raise NotImplementedError(f"Unknown storage type: {storage_yaml['type']}")


def find_all_backends(expr: ir.Expr) -> Tuple[BaseBackend, ...]:
    backends = set()
    seen = set()

    def traverse(node):
        if node is None or id(node) in seen:
            return
        seen.add(id(node))

        if isinstance(node, Table):
            traverse(node.op())
            return

        if isinstance(node, Read):
            backend = node.source
            if backend is not None:
                backends.add(backend)

        elif isinstance(node, ops.DatabaseTable):
            backends.add(node.source)

        elif isinstance(node, ops.SQLQueryResult):  # caching_utils uses
            backends.add(node.source)

        elif isinstance(node, CachedNode):
            backends.add(node.source)

        if isinstance(node, ops.Node):
            for arg in node.args:
                if isinstance(arg, ops.Node):
                    traverse(arg)
                elif isinstance(arg, (list, tuple)):
                    for item in arg:
                        if isinstance(item, ops.Node):
                            traverse(item)
                elif isinstance(arg, dict):
                    for v in arg.values():
                        if isinstance(v, ops.Node):
                            traverse(v)

    traverse(expr)

    return tuple(backends)


def find_relations(expr: ir.Expr) -> List[str]:
    relations = []
    seen = set()

    def traverse(node):
        if node is None or id(node) in seen:
            return
        seen.add(id(node))

        if isinstance(node, ops.Node):
            if isinstance(node, RemoteTable):
                relations.append(node.name)
            elif isinstance(node, Read):
                relations.append(node.make_unbound_dt().name)
            elif isinstance(node, ops.DatabaseTable):
                relations.append(node.name)

            for arg in node.args:
                if isinstance(arg, ops.Node):
                    traverse(arg)
                elif isinstance(arg, (list, tuple)):
                    for item in arg:
                        if isinstance(item, ops.Node):
                            traverse(item)
                elif isinstance(arg, dict):
                    for v in arg.values():
                        if isinstance(v, ops.Node):
                            traverse(v)

    traverse(expr.op())
    return list(dict.fromkeys(relations))
