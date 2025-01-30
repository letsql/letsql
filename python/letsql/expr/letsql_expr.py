import functools
from typing import Any, Generic, TypeVar

import ibis.common.exceptions as ibis_exc
import ibis.expr.types as ir

from letsql.common.caching import maybe_prevent_cross_source_caching
from letsql.common.utils.caching_utils import find_backend
from letsql.config import _backend_init
from letsql.expr.relations import CachedNode, into_backend


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

    def execute(self, **kwargs: Any):
        # avoid circular import
        from letsql.expr.api import execute

        return execute(self._ibis_expr, **kwargs)

    def into_backend(self, backend, name):
        new_ibis_expr = into_backend(self._ibis_expr, backend, name)
        return LetSQLExpr(new_ibis_expr)

    def cache(self, storage=None) -> "LetSQLExpr":
        try:
            current_backend, _ = find_backend(self._ibis_expr.op(), use_default=True)
        except ibis_exc.IbisError as e:
            if "Multiple backends found" in str(e):
                current_backend = _backend_init()
            else:
                raise

        if storage is None:
            from letsql.common.caching import SourceStorage

            storage = SourceStorage(source=current_backend)

        new_expr = maybe_prevent_cross_source_caching(self._ibis_expr, storage)

        op = CachedNode(
            schema=new_expr.schema(),
            parent=new_expr,
            source=current_backend,
            storage=storage,
        )
        cached_expr = op.to_expr()

        return LetSQLExpr(cached_expr)
