# https://stackoverflow.com/questions/68893521/simple-example-of-pandas-extensionarray
from __future__ import annotations

import operator
import re
from cloudpickle import (
    dumps,
    loads,
)
from typing import Any, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa


@pd.api.extensions.register_extension_dtype
class MyDtype(pd.core.dtypes.dtypes.PandasExtensionDtype):
    """
    An ExtensionDtype for unit-aware angular data.
    """

    _match = re.compile(r"(M|m)y")

    def __init__(self):
        pass

    def __str__(self) -> str:
        return "my"

    # TestDtypeTests
    def __hash__(self) -> int:
        return hash(str(self))

    # TestDtypeTests
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return self.name == other
        else:
            return isinstance(other, type(self))

    # Required for pickle compat (see GH26067)
    def __setstate__(self) -> None:
        pass

    # Required for all ExtensionDtype subclasses
    @classmethod
    def construct_array_type(cls):
        """
        Return the array type associated with this dtype.
        """
        return MyArray

    # Recommended for parameterized dtypes
    @classmethod
    def construct_from_string(cls, string: str) -> MyDtype:
        """
        Construct an MyDtype from a string.

        Example
        -------
        >>> MyDtype.construct_from_string("my")
        my
        """
        if not isinstance(string, str):
            msg = f"'construct_from_string' expects a string, got {type(string)}"
            raise TypeError(msg)

        msg = f"Cannot construct a '{cls.__name__}' from '{string}'"
        match = cls._match.match(string)

        if match:
            return cls()
        else:
            raise TypeError(msg)

    # Required for all ExtensionDtype subclasses
    @property
    def type(self):
        """
        The scalar type for the array (e.g., int).
        """
        return np.generic

    # Required for all ExtensionDtype subclasses
    @property
    def name(self) -> str:
        """
        A string representation of the dtype.
        """
        return str(self)

    @classmethod
    @property
    def ibis_name(cls):
        return cls.__name__.replace("Dtype", "")

    @classmethod
    def register_with_ibis(cls):
        import ibis.expr.datatypes as dt
        import ibis.formats.pandas as fp

        class MyIbisExtensionType(dt.Binary):
            pass

        # add custom type to ibis
        setattr(dt, cls.ibis_name, MyIbisExtensionType)
        assert fp.PandasType.to_ibis(cls()) == MyIbisExtensionType()

        # enable conversion on `.execute`
        override_attr = "from_ibis"
        from_ibis_original = getattr(fp.PandasType, override_attr)

        @classmethod
        def from_ibis_override(cls, dtype):
            if isinstance(dtype, MyIbisExtensionType):
                return MyArray.dtype
            else:
                return from_ibis_original(dtype)

        from_ibis_override.original = from_ibis_original
        setattr(fp.PandasType, override_attr, from_ibis_override)

    @classmethod
    def get_ibis_typ(cls):
        import ibis.expr.datatypes as dt

        return getattr(dt, cls.ibis_name)


class MyArray(pd.api.extensions.ExtensionArray):
    """
    An ExtensionArray for angular data.
    """

    # Include `copy` param for TestInterfaceTests
    def __init__(self, data, copy: bool = False):
        self._data = np.array(data, copy=copy)

    # Required for all ExtensionArray subclasses
    def __getitem__(self, index: int) -> MyArray | Any:
        """
        Select a subset of self.
        """
        if isinstance(index, int):
            return self._data[index]
        else:
            # Check index for TestGetitemTests
            index = pd.core.indexers.check_array_indexer(self, index)
            return type(self)(self._data[index])

    # TestSetitemTests
    def __setitem__(self, index: int, value: np.generic) -> None:
        """
        Set one or more values in-place.
        """
        # Check index for TestSetitemTests
        index = pd.core.indexers.check_array_indexer(self, index)

        # Upcast to value's type (if needed) for TestMethodsTests
        if self._data.dtype < type(value):
            self._data = self._data.astype(type(value))

        # TODO: Validate value for TestSetitemTests
        # value = self._validate_setitem_value(value)

        self._data[index] = value

    # Required for all ExtensionArray subclasses
    def __len__(self) -> int:
        """
        Length of this array.
        """
        return len(self._data)

    # TestUnaryOpsTests
    def __invert__(self) -> MyArray:
        """
        Element-wise inverse of this array.
        """
        data = ~self._data
        return type(self)(data)

    def _apply_operator(self, op, other, recast=False) -> np.ndarray | MyArray:
        """
        Helper method to apply an operator `op` between `self` and `other`.

        Some ops require the result to be recast into MyArray:
        * Comparison ops: recast=False
        * Arithmetic ops: recast=True
        """
        f = operator.attrgetter(op)
        data, other = np.array(self), np.array(other)
        result = f(data)(other)
        return result if not recast else type(self)(result)

    def _apply_operator_if_not_series(
        self, op, other, recast=False
    ) -> np.ndarray | MyArray:
        """
        Wraps _apply_operator only if `other` is not Series/DataFrame.

        Some ops should return NotImplemented if `other` is a Series/DataFrame:
        https://github.com/pandas-dev/pandas/blob/e7e7b40722e421ef7e519c645d851452c70a7b7c/pandas/tests/extension/base/ops.py#L115
        """
        if isinstance(other, (pd.Series, pd.DataFrame)):
            return NotImplemented
        else:
            return self._apply_operator(op, other, recast=recast)

    # Required for all ExtensionArray subclasses
    @pd.core.ops.unpack_zerodim_and_defer("__eq__")
    def __eq__(self, other):
        return self._apply_operator("__eq__", other, recast=False)

    # TestComparisonOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__ne__")
    def __ne__(self, other):
        return self._apply_operator("__ne__", other, recast=False)

    # TestComparisonOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__lt__")
    def __lt__(self, other):
        return self._apply_operator("__lt__", other, recast=False)

    # TestComparisonOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__gt__")
    def __gt__(self, other):
        return self._apply_operator("__gt__", other, recast=False)

    # TestComparisonOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__le__")
    def __le__(self, other):
        return self._apply_operator("__le__", other, recast=False)

    # TestComparisonOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__ge__")
    def __ge__(self, other):
        return self._apply_operator("__ge__", other, recast=False)

    # TestArithmeticOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__add__")
    def __add__(self, other) -> MyArray:
        return self._apply_operator_if_not_series("__add__", other, recast=True)

    # TestArithmeticOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__sub__")
    def __sub__(self, other) -> MyArray:
        return self._apply_operator_if_not_series("__sub__", other, recast=True)

    # TestArithmeticOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__mul__")
    def __mul__(self, other) -> MyArray:
        return self._apply_operator_if_not_series("__mul__", other, recast=True)

    # TestArithmeticOpsTests
    @pd.core.ops.unpack_zerodim_and_defer("__truediv__")
    def __truediv__(self, other) -> MyArray:
        return self._apply_operator_if_not_series("__truediv__", other, recast=True)

    # Required for all ExtensionArray subclasses
    @classmethod
    def _from_sequence(cls, data, dtype=None, copy: bool = False):
        """
        Construct a new MyArray from a sequence of scalars.
        """
        if dtype is None:
            dtype = cls.dtype

        if not isinstance(dtype, type(cls.dtype)):
            msg = (
                f"'{cls.__name__}' only supports '{cls.dtype.__class__.__name__}' dtype"
            )
            raise ValueError(msg)
        else:
            return cls(data, copy=copy)

    # TestParsingTests
    @classmethod
    def _from_sequence_of_strings(
        cls, strings, *, dtype=None, copy: bool = False
    ) -> MyArray:
        """
        Construct a new MyArray from a sequence of strings.
        """
        scalars = pd.to_numeric(strings, errors="raise")
        return cls._from_sequence(scalars, dtype=dtype, copy=copy)

    # Required for all ExtensionArray subclasses
    @classmethod
    def _from_factorized(cls, uniques: np.ndarray, original: MyArray):
        """
        Reconstruct an MyArray after factorization.
        """
        return cls(uniques)

    # Required for all ExtensionArray subclasses
    @classmethod
    def _concat_same_type(cls, to_concat: Sequence[MyArray]) -> MyArray:
        """
        Concatenate multiple MyArrays.
        """
        return cls(np.concatenate(to_concat))

    # Required for all ExtensionArray subclasses
    @classmethod
    @property
    def dtype(cls):
        """
        An instance of MyDtype.
        """
        return MyDtype()

    # Required for all ExtensionArray subclasses
    @property
    def nbytes(self) -> int:
        """
        The number of bytes needed to store this object in memory.
        """
        return self._data.nbytes

    # Test*ReduceTests
    def all(self) -> bool:
        return all(self)

    def any(self) -> bool:  # Test*ReduceTests
        return any(self)

    def sum(self) -> np.generic:  # Test*ReduceTests
        return self._data.sum()

    def mean(self) -> np.generic:  # Test*ReduceTests
        return self._data.mean()

    def max(self) -> np.generic:  # Test*ReduceTests
        return self._data.max()

    def min(self) -> np.generic:  # Test*ReduceTests
        return self._data.min()

    def prod(self) -> np.generic:  # Test*ReduceTests
        return self._data.prod()

    def std(self) -> np.generic:  # Test*ReduceTests
        return pd.Series(self._data).std()

    def var(self) -> np.generic:  # Test*ReduceTests
        return pd.Series(self._data).var()

    def median(self) -> np.generic:  # Test*ReduceTests
        return np.median(self._data)

    def skew(self) -> np.generic:  # Test*ReduceTests
        return pd.Series(self._data).skew()

    def kurt(self) -> np.generic:  # Test*ReduceTests
        return pd.Series(self._data).kurt()

    # Test*ReduceTests
    def _reduce(self, name: str, *, skipna: bool = True, **kwargs):
        """
        Return a scalar result of performing the reduction operation.
        """
        f = operator.attrgetter(name)
        return f(self)()

    # Required for all ExtensionArray subclasses
    def isna(self):
        """
        A 1-D array indicating if each value is missing.
        """
        return pd.isnull(self._data)

    # Required for all ExtensionArray subclasses
    def copy(self):
        """
        Return a copy of the array.
        """
        copied = self._data.copy()
        return type(self)(copied)

    # Required for all ExtensionArray subclasses
    def take(self, indices, allow_fill=False, fill_value=None):
        """
        Take elements from an array.
        """
        if allow_fill and fill_value is None:
            fill_value = self.dtype.na_value

        result = pd.core.algorithms.take(
            self._data, indices, allow_fill=allow_fill, fill_value=fill_value
        )
        return self._from_sequence(result)

    # TestMethodsTests
    def value_counts(self, dropna: bool = True):
        """
        Return a Series containing descending counts of unique values (excludes NA values by default).
        """
        return pd.core.algorithms.value_counts(self._data, dropna=dropna)

    def __arrow_array__(self, type=None):
        # https://arrow.apache.org/docs/python/extending_types.html#controlling-conversion-to-pyarrow-array-with-the-arrow-array-protocol
        if type is None:
            type = PickledType()
        return pa.array(
            map(dumps, self._data),
            type=type,
        )

    @classmethod
    def __from_arrow__(cls, obj):
        # https://arrow.apache.org/docs/python/extending_types.html#conversion-to-pandas
        return cls(obj.to_pylist())


class PickledScalar(pa.ExtensionScalar):
    # https://arrow.apache.org/docs/python/extending_types.html#custom-scalar-conversion
    def as_py(self):
        return loads(self.value.as_py())


class PickledType(pa.ExtensionType):
    # https://arrow.apache.org/docs/python/extending_types.html
    def __init__(self):
        super().__init__(pa.binary(), self._name)

    def __hash__(self):
        return hash(
            (
                self.__class__,
                self.storage_type,
            )
        )

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return PickledType()

    def __arrow_ext_scalar_class__(self):
        return PickledScalar

    def to_pandas_dtype(self):
        return MyArray

    @classmethod
    @property
    def _name(cls):
        return f"{cls.__module__}.{cls.__name__}"

    @classmethod
    def register(cls, ensure=True):
        try:
            pa.register_extension_type(cls())
        except pa.ArrowKeyError as e:
            if ensure:
                pass
            else:
                raise e

    @classmethod
    def unregister(cls, ensure=True):
        try:
            pa.unregister_extension_type(cls._name)
        except pa.ArrowKeyError as e:
            if ensure:
                pass
            else:
                raise e


MyDtype.register_with_ibis()
PickledType.register()
