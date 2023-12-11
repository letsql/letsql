from __future__ import annotations

import ibis.expr.operations as ops
from ibis.common.patterns import pattern, replace
from ibis.util import Namespace

from operator import add, lt

p = Namespace(pattern, module=ops)

ops_mapping = {ops.Less: lt, ops.Add: add}


@replace(p.Binary)
def evaluate_comparison_constant(_):
    if isinstance(_.left, ops.Literal) and isinstance(_.right, ops.Literal):
        op = ops_mapping.get(type(_), None)
        if op:
            return ops.Literal(value=op(_.left.value, _.right.value), dtype=_.dtype)
    return _


@replace(p.Selection)
def simplify_selection_predicates(_):
    if _.predicates:
        literal = next(
            (pred for pred in _.predicates if isinstance(pred, ops.Literal)), None
        )
        if literal and not literal.value:
            return _.copy(predicates=(literal,))

    return _
