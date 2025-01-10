from random import Random
from typing import Tuple, Iterable

# TODO: How should we / should we enforce letsql table ?
import ibis.expr.types as ir
from ibis import literal
import functools


def train_test_split(
    table: ir.Table,
    unique_key: str | list[str],
    test_size: float = 0.25,
    num_buckets: int = 10000,
    random_seed: int | None = None,
) -> Tuple[ir.Table, ir.Table]:
    """Randomly split Ibis table data into training and testing tables.

    This function splits an Ibis table into training and testing tables
    based on a unique key or combination of keys. It uses a hashing function to
    convert the unique key into an integer, then applies a modulo operation to split
    the data into buckets. The training table consists of data points from a subset of
    these buckets, while the remaining data points form the test table.

    Parameters
    ----------
    table
        The input Ibis table to be split.
    unique_key
        The column name(s) that uniquely identify each row in the table. This unique_key
        is used to create a deterministic split of the dataset through a hashing
        process.
    test_size
        The ratio of the dataset to include in the test split, which should be between
        0 and 1. This ratio is approximate because the hashing algorithm may not provide
        a uniform bucket distribution for small datasets. Larger datasets will result in
        more uniform bucket assignments, making the split ratio closer to the desired
        value.  This can be a float or a list of floats, if a list of floats is entered you will receive
        a train test split tuple for each of float.
    num_buckets
        The number of buckets into which the data can be binned after being hashed and taking the abs value.
        It controls how finely the data is divided into buckets during
        the split process. Adjusting num_buckets can affect the granularity and
        efficiency of the splitting operation, balancing between accuracy and
        computational efficiency. Note that the default is 10,000 if your data is smaller than this you should decrease the bucket size.
        Changing num_buckets will also potentially an observations membership.
    random_seed
        Seed for the random number generator. If provided, ensures reproducibility
        of the split.

    Returns
    -------
    tuple[ir.Table, ir.Table]
        A tuple containing two Ibis tables: (train_table, test_table).

    Raises
    ------
    ValueError
        If test_size is not a float between 0 and 1.

    Examples
    --------
    >>> import letsql as ls

    Split an Ibis table into training and testing tables.

    >>> table = ibis.memtable({"key1": range(100)})
    >>> train_table, test_table = ls.train_test_split(
    ...     table,
    ...     unique_key="key1",
    ...     test_size=0.2,
    ...     random_seed=0,
    ... )
    """
    if not (0 < test_size < 1):
        raise ValueError("test size should be a float between 0 and 1.")

    if not (isinstance(num_buckets, int)):
        raise ValueError("num_buckets must be an integer.")

    if not (num_buckets > 1 and isinstance(num_buckets, int)):
        raise ValueError(
            "num_buckets = 1 places all data into training set. For any integer x  >=0  , x mod 1 = 0 . "
        )

    # Set the random seed if set, & Generate a random 256-bit key

    random_str = str(Random(random_seed).getrandbits(256))

    if isinstance(unique_key, str):
        unique_key = [unique_key]

    # Append random string to the name to avoid collision
    train_flag = f"train_{random_str}"
    comb_key = literal(",").join(table[col].cast("str") for col in unique_key)

    table = table.mutate(
        **{
            train_flag: (comb_key + random_str).hash().abs() % num_buckets
            < int((1 - test_size) * num_buckets)
        }
    )

    return (
        table.filter(table[train_flag]).drop([train_flag]),
        table.filter(~table[train_flag]).drop([train_flag]),
    )


def train_test_splits(
    table: ir.Table,
    unique_key: str | list[str],
    test_sizes: list[float],
    num_buckets: int = 100,
    random_seed: int | None = None,
) -> Iterable[Tuple[ir.Table, ir.Table]]:
    """
    Generates multiple train/test splits of an Ibis table for different test sizes.


    Parameters
    ----------
    table : ir.Table
        The Ibis table to split.
    unique_key : str | list[str]
        The column name(s) that uniquely identify each row.
    test_sizes : list[float]
        A list of test sizes (proportions between 0 and 1) for which to generate splits.
    num_buckets : int, optional
        The number of buckets to use for hashing (default is 100).
    random_seed : int | None, optional
        The random seed for reproducibility (default is None).

    Returns
    -------
    Iterable[Tuple[ir.Table, ir.Table]]
        An iterable of (train_table, test_table) tuples, one for each test size.
    """
    # Create a partially applied function with fixed arguments
    partial_train_test_split = functools.partial(
        train_test_split,
        table=table,
        unique_key=unique_key,
        num_buckets=num_buckets,
        random_seed=random_seed,
    )
    if not all(isinstance(item, float) for item in test_sizes):
        raise ValueError(" test_sizes needs to be a list of floats.")
    # Yield train/test splits for each test size
    for test_size in test_sizes:
        yield partial_train_test_split(test_size=test_size)
