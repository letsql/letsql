from __future__ import annotations

import functools
import inspect
import typing
from typing import TYPE_CHECKING, Any

import ibis.common.exceptions as exc
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
from ibis.common.annotations import Argument
from ibis.common.collections import FrozenDict
from ibis.expr.operations import Namespace
from ibis.expr.operations.udf import _UDF, AggUDF, _wrap, InputType, _make_udf_name

if TYPE_CHECKING:
    from collections.abc import Callable

    import ibis.expr.types as ir


class agg(_UDF):
    __slots__ = ()

    _base = AggUDF

    @classmethod
    def pyarrow(
        cls,
        fn=None,
        name=None,
        signature=None,
        **kwargs,
    ):
        result = _wrap(
            cls._make_wrapper,
            InputType.PYARROW,
            fn,
            name=name,
            signature=signature,
            **kwargs,
        )
        return result

    @classmethod
    def pandas_df(
        cls,
        expr,
        fn,
        return_type,
        database=None,
        catalog=None,
        name=None,
        **kwargs,
    ):
        import pandas as pd

        def fn_from_arrays(*arrays):
            df = pd.DataFrame(
                {name: array.to_pandas() for (name, array) in zip(expr.columns, arrays)}
            )
            return fn(df)

        name = name if name is not None else _make_udf_name(fn)
        bases = (cls._base,)
        fields = {
            arg_name: Argument(pattern=rlz.ValueOf(typ), typehint=typ)
            for (arg_name, typ) in expr.schema().items()
        }
        meta = {
            "dtype": return_type,
            "__input_type__": InputType.PYARROW,
            "__func__": property(
                fget=lambda _, fn_from_arrays=fn_from_arrays: fn_from_arrays
            ),
            # valid config keys: volatility
            "__config__": FrozenDict(**kwargs),
            "__udf_namespace__": Namespace(database=database, catalog=catalog),
            "__module__": fn.__module__,
            # FIXME: determine why this fails with case mismatch by default
            "__func_name__": name,
        }
        kwds = {
            **fields,
            **meta,
        }

        node = type(
            name,
            bases,
            kwds,
        )

        # FIXME: enable working with deferred like _wrap enables
        @functools.wraps(fn)
        def construct(*args: Any, **kwargs: Any) -> ir.Value:
            return node(*args, **kwargs).to_expr()

        def on_expr(e, **kwargs):
            return construct(*(e[c] for c in expr.columns), **kwargs)

        construct.on_expr = on_expr
        return construct


class window(_UDF):
    __slots__ = ()
    _base = AggUDF

    @classmethod
    def _make_node(
        cls,
        fn: Callable,
        input_type: InputType,
        name: str | None = None,
        database: str | None = None,
        catalog: str | None = None,
        signature: tuple[tuple, Any] | None = None,
        **kwargs,
    ):
        if signature is None:
            annotations = typing.get_type_hints(fn)
            if (return_annotation := annotations.pop("return", None)) is None:
                raise exc.MissingReturnAnnotationError(fn)
            fields = {
                arg_name: Argument(
                    pattern=rlz.ValueOf(annotations.get(arg_name)),
                    default=param.default,
                    typehint=annotations.get(arg_name, Any),
                )
                for arg_name, param in inspect.signature(fn).parameters.items()
            }

        else:
            arg_types, return_annotation = signature
            arg_names = list(inspect.signature(fn).parameters)
            fields = {
                arg_name: Argument(pattern=rlz.ValueOf(typ), typehint=typ)
                for arg_name, typ in zip(arg_names, arg_types)
            }

        func_name = name if name is not None else fn.__name__

        fields.update(
            {
                "dtype": dt.dtype(return_annotation),
                "__input_type__": input_type,
                "__func__": property(fget=lambda _, fn=fn: fn),
                "__config__": FrozenDict(kwargs),
                "__udf_namespace__": ops.Namespace(database=database, catalog=catalog),
                "__module__": fn.__module__,
                "__func_name__": func_name,
                "__evaluation_method__": "evaluate_all",
            }
        )

        return type(_make_udf_name(fn.__name__), (cls._base,), fields)

    @classmethod
    def pyarrow(
        cls,
        fn=None,
        name=None,
        signature=None,
        **kwargs,
    ):
        result = _wrap(
            cls._make_wrapper,
            InputType.PYARROW,
            fn,
            name=name,
            signature=signature,
            **kwargs,
        )
        return result
