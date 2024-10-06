from functools import update_wrapper
from typing import Callable

import ibis as ix


def compose(func, other, *args, **keywords):
    def composition(self, *a, **kw):
        return other(func(self, *a, **kw), *args, **keywords)

    return composition


class PartialExpr:
    def __init__(self, func: Callable):
        update_wrapper(self, func)
        self.func = func

    def _compose(self, other, *args, **kwargs):
        return PartialExpr(compose(self.func, other, *args, **kwargs))

    def limit(self, n: int | None, offset: int = 0) -> "PartialExpr":
        return self._compose(ix.Table.limit, n, offset)

    def join(
        self,
        right: ix.Table,
        predicates=(),
        how="inner",
        *,
        lname: str = "",
        rname: str = "{name}_right",
    ) -> "PartialExpr":
        return self._compose(
            ix.Table.join,
            right,
            predicates=predicates,
            how=how,
            lname=lname,
            rname=rname,
        )

    def select(
        self,
        *exprs,
        **named_exprs,
    ) -> "PartialExpr":
        return self._compose(ix.Table.select, *exprs, **named_exprs)

    def mutate(self, *exprs, **mutations) -> "PartialExpr":
        return self._compose(ix.Table.mutate, *exprs, **mutations)

    def filter(self, *predicates) -> "PartialExpr":
        return self._compose(ix.Table.filter, *predicates)

    def head(self, n: int = 5) -> "PartialExpr":
        return self._compose(ix.Table.head, n)

    def __call__(self, *args, **keywords):
        def bound(other, *a, **kw):
            return self.func(other, *args, *a, **keywords, **kw)

        return PartialExpr(bound)

    def __ror__(self, value):
        return self.func(value)


select = PartialExpr(ix.Table.select)
limit = PartialExpr(ix.Table.limit)
join = PartialExpr(ix.Table.join)
mutate = PartialExpr(ix.Table.mutate)
head = PartialExpr(ix.Table.head)
filter = PartialExpr(ix.Table.filter)
sql = PartialExpr(ix.Table.sql)

_ = ix._
