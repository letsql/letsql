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
def simplify_selection(_):
    if _.predicates:
        literal = next(
            (pred for pred in _.predicates if isinstance(pred, ops.Literal)), None
        )
        if literal and not literal.value:
            return _.copy(predicates=(literal,))

        equal_predicates = [
            pred for pred in _.predicates if isinstance(pred, ops.Equals)
        ]
        replacements = {
            pred.left: pred.right
            for pred in equal_predicates
            if isinstance(pred.left, ops.TableColumn)
            and isinstance(pred.right, ops.Literal)
        }

        if _.selections:
            return _.copy(
                selections=tuple(replacements.get(s, s) for s in _.selections)
            )

    return _
