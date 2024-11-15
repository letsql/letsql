from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any

import ibis.expr.rules as rlz
from ibis.common.annotations import Argument
from ibis.common.collections import FrozenDict
from ibis.expr.operations import Namespace
from ibis.expr.operations.udf import _UDF, AggUDF, _wrap, InputType, _make_udf_name
import ibis.expr.datatypes as dt

if TYPE_CHECKING:
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
    def pyarrow(
        cls,
        fn=None,
        name=None,
        input_types: dt.DataType | list[dt.DataType] = None,
        return_type: dt.DataType = None,
        namespace=Namespace(database=None, catalog=None),
        **kwargs,
    ):
        if name is None:
            raise ValueError("The name must be provided.")

        if input_types is None:
            raise ValueError("The input_types must be provided.")

        if return_type is None:
            raise ValueError("The return_type must be provided.")

        if isinstance(input_types, dt.DataType):
            input_types = [input_types]

        fields = {
            f"arg_{i}": Argument(pattern=rlz.ValueOf(typ), typehint=typ)
            for i, typ in enumerate(input_types)
        }

        def getter(_self):
            return fn

        meta = {
            "dtype": return_type,
            "__input_type__": InputType.PYARROW,
            "__func__": property(fget=getter),
            "__func_name__": name,
            "__config__": FrozenDict(kwargs),
            "__udf_namespace__": namespace,
            "__module__": fn.__module__,
        }
        node = type(
            name,
            (cls._base,),
            {
                **fields,
                **meta,
            },
        )

        def construct(*args: Any, **kwargs: Any) -> ir.Value:
            return node(*args, **kwargs).to_expr()

        return construct
