import functools
from typing import Generic, TypeVar

import ibis.common.exceptions as ibis_exc
import ibis.expr.types as ir


T = TypeVar("T", bound=ir.Expr)


def wrap_ibis_function(func):
    """Decorator to wrap an Ibis function so it raises `LetSQLError`."""

    @functools.wraps(func)
    def _wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ibis_exc.IbisError as e:
            raise LetSQLError(f"Error in {func.__name__}: {e}") from e

    return _wrapped


class LetSQLError(Exception):
    """All user-facing errors for LetSQL."""

    pass


class LetSQLExpr(Generic[T]):
    """LetSQL expression wrapper around a raw Ibis expression."""

    __slots__ = ("_ibis_expr",)

    def __init__(self, ibis_expr: T):
        self._ibis_expr = ibis_expr

    def __repr__(self):
        return f"<LetSQLExpr: {self._ibis_expr!r}>"
