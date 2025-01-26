import functools
import inspect
import types
from pathlib import Path
from random import Random
from typing import TYPE_CHECKING, Callable, Iterable, Iterator, List, Tuple, Union

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops

# TODO: How should we / should we enforce letsql table ?
import ibis.expr.types as ir
from ibis import literal
from ibis.common.annotations import Argument
from ibis.common.collections import FrozenDict
from ibis.common.patterns import pattern, replace
from ibis.expr.operations.udf import InputType, ScalarUDF
from ibis.expr.rules import ValueOf
from ibis.util import Namespace

if TYPE_CHECKING:
    from quickgrove import PyGradientBoostedDecisionTrees

p = Namespace(pattern, module=ops)

def _calculate_bounds(test_sizes: List[float]) -> List[Tuple[float, float]]:
    """
    Calculates the cumulative sum of test_sizes and generates bounds for splitting data.

    Parameters
    ----------
    test_sizes : List[float]
        A list of floats representing the desired proportions for data splits.
        Each value should be between 0 and 1, and their sum should ideally be less than or equal to 1.

    Returns
    -------
    List[Tuple[float, float]]
        A list of tuples, where each tuple contains two floats representing the
        lower and upper bounds for a split. These bounds are calculated based on
        the cumulative sum of the `test_sizes`.
    """

    num_splits = len(test_sizes)
    cumulative_sizes = [sum(test_sizes[: i + 1]) for i in range(num_splits)]
    cumulative_sizes.insert(0, 0.0)
    bounds = [(cumulative_sizes[i], cumulative_sizes[i + 1]) for i in range(num_splits)]
    return bounds


def train_test_splits(
    table: ir.Table,
    unique_key: str | list[str],
    test_sizes: Iterable[float] | float,
    num_buckets: int = 10000,
    random_seed: int | None = None,
) -> Iterator[ir.Table]:
    """Generates multiple train/test splits of an Ibis table for different test sizes.

    This function splits an Ibis table into multiple subsets based on a unique key
    or combination of keys and a list of test sizes. It uses a hashing function to
    convert the unique key into an integer, then applies a modulo operation to split
    the data into buckets. Each subset of data is defined by a range of
    buckets determined by the cumulative sum of the test sizes.

    Parameters
    ----------
    table : ir.Table
        The input Ibis table to be split.
    unique_key : str | list[str]
        The column name(s) that uniquely identify each row in the table. This
        unique_key is used to create a deterministic split of the dataset
        through a hashing process.
    test_sizes : Iterable[float] | float
        An iterable of floats representing the desired proportions for data splits.
        Each value should be between 0 and 1, and their sum must equal 1. The
        order of test sizes determines the order of the generated subsets. If float is passed
        it assumes that the value is for the test size and that a tradition tain test split of (1-test_size, test_size) is returned.
    num_buckets : int, optional
        The number of buckets into which the data can be binned after being
        hashed (default is 10000). It controls how finely the data is divided
        during the split process. Adjusting num_buckets can affect the
        granularity and efficiency of the splitting operation, balancing
        between accuracy and computational efficiency.
    random_seed : int | None, optional
        Seed for the random number generator. If provided, ensures
        reproducibility of the split (default is None).

    Returns
    -------
    Iterator[ir.Table]
        An iterator yielding Ibis table expressions, each representing a mutually exclusive
        subset of the original table based on the specified test sizes.

    Raises
    ------
    ValueError
        If any value in `test_sizes` is not between 0 and 1.
        If `test_sizes` does not sum to 1.
        If `num_buckets` is not an integer greater than 1.

    Examples
    --------
    >>> import letsql as ls
    >>> import ibis
    >>> table = ibis.memtable({"key": range(100), "value": range(100,200)})
    >>> unique_key = "key"
    >>> test_sizes = [0.2, 0.3, 0.5]
    >>> splits = ls.train_test_splits(table, unique_key, test_sizes, num_buckets=10, random_seed=42)
    >>> for i, split_table in enumerate(splits):
    ...     print(f"Split {i+1} size: {split_table.count().execute()}")
    ...     print(split_table.execute())
    Split 1 size: 20
    Split 2 size: 30
    Split 3 size: 50
    """
    # Convert to traditional train test split
    if isinstance(test_sizes, float):
        test_sizes = [1 - test_sizes, test_sizes]

    if not all(isinstance(test_size, float) for test_size in test_sizes):
        raise ValueError("Test size must be float.")

    if not all((0 < test_size < 1) for test_size in test_sizes):
        raise ValueError("test size should be a float between 0 and 1.")

    if not (isinstance(num_buckets, int)):
        raise ValueError("num_buckets must be an integer.")

    if not (num_buckets > 1 and isinstance(num_buckets, int)):
        raise ValueError(
            "num_buckets = 1 places all data into training set. For any integer x  >=0 , x mod 1 = 0 . "
        )

    if not sum(test_sizes) == 1:
        raise ValueError("Test sizes must sum to 1")

    # Get cumulative bounds
    bounds = _calculate_bounds(test_sizes=test_sizes)

    # Set the random seed if set, & Generate a random 256-bit key
    random_str = str(Random(random_seed).getrandbits(256))
    hash_name = f"hash_{random_str}"

    if isinstance(unique_key, str):
        unique_key = [unique_key]

    comb_key = literal(",").join(table[col].cast("str") for col in unique_key)

    table = table.mutate(
        **{hash_name: (comb_key + random_str).hash().abs() % num_buckets}
    )
    train_test_filters = [
        (
            literal(bound[0]).cast("decimal(38, 9)") * num_buckets <= table[hash_name]
        )  # lower bound condition
        & (
            table[hash_name] < literal(bound[1]).cast("decimal(38, 9)") * num_buckets
        )  # upper bound condition
        for i, bound in enumerate(bounds)
    ]

    return (table.filter(_filter).drop([hash_name]) for _filter in train_test_filters)


def fields_to_parameters(fields):
    parameters = []
    for name, arg in fields.items():
        param = inspect.Parameter(
            name, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=arg.typehint
        )
        parameters.append(param)
    return parameters


class UDFWrapper:
    """need this for repr of the udf"""

    def __init__(self, func, model, model_path):
        self.func = func
        self.model = model
        self.model_path = model_path
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return f"{str(self.model)}\nModel path: {self.model_path}\nSignature: {str(self.func.__signature__)}"


def make_xgboost_udf(model_path: Union[str, Path]) -> Callable:
    """Create a UDF from an XGBoost model.

    Parameters
    ----------
    model_path : str | Path
        Path to the JSON file containing the XGBoost model

    Returns
    -------
    Callable
        A function that can be used to call the XGBoost model as a scalar pyarrow UDF

    Raises
    ------
    ValueError
        If model contains features with unsupported types
    """
    import quickgrove

    model = quickgrove.json_load(model_path)

    SUPPORTED_TYPES = {
        "float": dt.float64,
        "i": dt.boolean,
        "int": dt.int64,
    }

    required_features = sorted(model.required_features)
    schema = [model.feature_names[i] for i in required_features]
    feature_types = [model.feature_types[i] for i in required_features]

    unsupported = [
        f"{name}: {type_}"
        for name, type_ in zip(schema, feature_types)
        if type_ not in SUPPORTED_TYPES
    ]
    if unsupported:
        raise ValueError(f"Unsupported feature types: {', '.join(unsupported)}")

    fields = {
        name: Argument(
            pattern=ValueOf(SUPPORTED_TYPES[type_]), typehint=SUPPORTED_TYPES[type_]
        )
        for name, type_ in zip(schema, feature_types)
    }

    def fn_from_arrays(*arrays):
        return model.predict_arrays(list(arrays))

    meta = {
        "dtype": dt.float32,
        "__input_type__": InputType.PYARROW,
        "__func__": property(lambda self: fn_from_arrays),
        "__config__": FrozenDict(volatility="immutable"),
        "__udf_namespace__": Namespace(pattern, module=ops),
        "__module__": "letsql.expr.ml",
        "__func_name__": "predict_xgboost",  # what should this be?
    }

    node = type("XGBPredict", (ScalarUDF,), {**fields, **meta})

    def create_named_wrapper(func, name, signature):
        def predict_xgboost(*args, **kwargs):
            return func(*args, **kwargs)

        new_func = types.FunctionType(
            predict_xgboost.__code__,
            predict_xgboost.__globals__,
            name=name,
            argdefs=predict_xgboost.__defaults__,
            closure=predict_xgboost.__closure__,
        )
        new_func.__signature__ = signature
        return new_func

    @functools.wraps(fn_from_arrays)
    def construct(*args, **kwargs):
        return node(*args, **kwargs).to_expr()

    def on_expr(e, **kwargs):
        return construct(*(e[c] for c in schema), **kwargs)

    signature = inspect.Signature(
        fields_to_parameters(fields), return_annotation=dt.float32
    )
    wrapper = create_named_wrapper(construct, "predict_xgboost", signature)

    wrapper.fn = fn_from_arrays
    wrapper.on_expr = on_expr
    wrapper.model = model
    wrapper.feature_names = schema
    wrapper.feature_types = feature_types

    return UDFWrapper(wrapper, model, model_path)


def collect_predicates(filter_op: ops.Filter) -> List[dict]:
    """Extract predicates that can be pushed down (Less, Greater, GreaterEqual) where left side is a Field."""
    predicates = []
    for pred in filter_op.predicates:
        if isinstance(pred.left, ops.Field) and isinstance(pred.right, ops.Literal):
            if isinstance(pred, (ops.Less, ops.Greater, ops.GreaterEqual)):
                predicates.append(
                    {
                        "column": pred.left.name,
                        "op": pred.__class__.__name__,
                        "value": pred.right.value,
                    }
                )
    return predicates


def make_pruned_udf(
    original_udf: callable,
    model: 'PyGradientBoostedDecisionTrees',
    predicates: List[dict],
) -> Tuple[callable, List[str]]:
    """Create a new UDF using the pruned model based on predicates."""
    from quickgrove import Feature
    pruned_model = model.prune(
        [
            Feature(pred["column"]) < pred["value"]
            if pred["op"] == "Less"
            else Feature(pred["column"]) > pred["value"]
            if pred["op"] == "Greater"
            else Feature(pred["column"]) >= pred["value"]
            for pred in predicates
        ]
    )

    required_features = sorted(pruned_model.required_features)
    feature_names = [model.feature_names[i] for i in required_features]

    def fn_from_arrays(*arrays):
        return pruned_model.predict_arrays(list(arrays))

    fields = {
        feature_name: Argument(pattern=ValueOf(dt.float64), typehint=dt.float64)
        for feature_name in feature_names
    }

    meta = {
        "dtype": dt.float32,
        "__input_type__": InputType.PYARROW,
        "__func__": property(lambda self: fn_from_arrays),
        "__config__": FrozenDict(volatility="immutable"),
        "__udf_namespace__": Namespace(pattern, module=ops),
        "__module__": original_udf.__module__,
        "__func_name__": original_udf.__name__ + "_pruned",
    }

    node = type(original_udf.__name__ + "_pruned", (ScalarUDF,), {**fields, **meta})

    @functools.wraps(fn_from_arrays)
    def construct(*args, **kwargs):
        return node(*args, **kwargs).to_expr()

    construct.fn = fn_from_arrays
    return construct, feature_names


# @pattern.replace(p.Filter(p.Project(values=lambda x: any(
#     isinstance(v, ops.ScalarUDF) and hasattr(v.__func__, 'model')
#     for v in x.values()
# ))))
@replace(p.Filter(p.Project))
def prune_gbdt_model(_, **kwargs):
    """Rewrite rule to prune GBDT model based on filter predicates.
    Only matches Filter operations that have a Project as their parent and contain
    'Less' predicates with Field operations on the left side.
    """
    model = kwargs["model"]
    original_udf = kwargs["original_udf"]

    # Check for blocking operations : is this really needed?
    blocking = (ops.WindowFunction, ops.ExistsSubquery, ops.InSubquery)
    if _.find_below(blocking, filter=ops.Value):
        return _

    predicates = collect_predicates(_)
    if not predicates:
        return _

    pruned_udf, required_features = make_pruned_udf(original_udf, model, predicates)
    parent_op = _.parent

    new_values = {}
    for name, value in parent_op.values.items():
        if name == "prediction":
            udf_kwargs = {
                feat_name: parent_op.values[feat_name]
                for feat_name in required_features
            }
            new_values[name] = pruned_udf(**udf_kwargs)
        else:
            new_values[name] = value

    new_project = ops.Project(parent_op.parent, new_values)

    subs = {
        ops.Field(parent_op, k): ops.Field(new_project, k) for k in parent_op.values
    }
    new_predicates = tuple(p.replace(subs, filter=ops.Value) for p in _.predicates)

    return ops.Filter(parent=new_project, predicates=new_predicates)


def rewrite_gbdt_expression(expr):
    """rewrite an Ibis expression by pruning GBDT models based on filter conditions."""
    op = expr.op()

    def find_prunable_udfs(node, kwargs=None):
        if isinstance(node, ops.ScalarUDF) and getattr(
            node.__func__, "needs_pruning", False
        ):
            udf = node.__func__
            new_op = node.replace(
                prune_gbdt_model, context={"model": udf.model, "original_udf": udf}
            )
            return new_op.to_expr()
        if kwargs:
            return node.__recreate__(kwargs)
        return node

    return op.replace(find_prunable_udfs).to_expr()
