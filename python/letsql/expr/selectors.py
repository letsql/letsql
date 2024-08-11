from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Optional, Union, List

import ibis.selectors as s
import ibis.expr.types as ir
from ibis.common.collections import frozendict  # noqa: TCH001
from ibis.common.deferred import Deferred, Resolver
from ibis.selectors import Selector
from ibis import util
from public import public


class Pick(Selector):
    selector: Selector
    funcs: Union[
        Resolver,
        Callable[[List[ir.Value]], ir.Value],
        frozendict[
            Optional[str], Union[Resolver, Callable[[List[ir.Value]], ir.Value]]
        ],
    ]
    names: Union[str, Callable[[str, Optional[str]], str]]

    def expand(self, table: ir.Table) -> Sequence[ir.Value]:
        expanded = []

        names = self.names
        cols = self.selector.expand(table)
        for func_name, func in self.funcs.items():
            if isinstance(func, Resolver):
                col = func.resolve({"_": cols})
            else:
                col = func(cols)

            if callable(names):
                name = names(col.get_name(), func_name)
            else:
                name = names.format(col=col.get_name(), fn=func_name)
            expanded.append(col.name(name))
        return expanded


@public
def pick(
    selector: Selector | Iterable[str] | str,
    func: (
        Deferred
        | Callable[[List[ir.Value]], ir.Value]
        | Mapping[str | None, Deferred | Callable[[List[ir.Value]], ir.Value]]
    ),
    names: str | Callable[[str, str | None], str] | None = None,
) -> Pick:
    if names is None:
        names = lambda col, fn: "_".join(filter(None, (col, fn)))
    funcs = dict(func if isinstance(func, Mapping) else {None: func})
    if not isinstance(selector, Selector):
        selector = s.c(*util.promote_list(selector))
    return Pick(selector=selector, funcs=funcs, names=names)
