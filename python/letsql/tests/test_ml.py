from __future__ import annotations

from ibis import memtable
import pytest
import letsql as ls

# from letsql.tests.conftest import TEST_TABLES
from letsql.tests.util import assert_frame_equal


def test_train_test_split_new1():
    # Check counts and overlaps in train and test dataset
    N = 100
    test_size = 0.25

    # init table
    table = memtable([(i, "val") for i in range(N)], columns=["key1", "val"])
    train_table, test_table = ls.train_test_split(
        table, unique_key="key1", test_size=test_size, num_buckets=N, random_seed=42
    )
    # These values are for seed 42
    assert train_table.count().execute() == 73
    assert test_table.count().execute() == 27
    assert set(train_table.columns) == set(table.columns)
    assert set(test_table.columns) == set(table.columns)
    # make sure data unioned together is itself
    assert train_table.union(test_table).join(table, how="semi").count().execute() == N

    # Check reproducibility
    reproduced_train_table, reproduced_test_table = ls.train_test_split(
        table, unique_key="key1", test_size=test_size, num_buckets=N, random_seed=42
    )
    assert_frame_equal(train_table.execute(), reproduced_train_table.execute())
    assert_frame_equal(test_table.execute(), reproduced_test_table.execute())

    # make sure it could generate different data with different random_seed
    different_train_table, different_test_table = ls.train_test_split(
        table, unique_key="key1", test_size=test_size, num_buckets=N, random_seed=0
    )
    assert not train_table.execute().equals(different_train_table.execute())
    assert not test_table.execute().equals(different_test_table.execute())


def test_train_test_split_invalid_test_size():
    table = memtable({"key": [1, 2, 3]})
    with pytest.raises(ValueError, match="test size should be a float between 0 and 1"):
        ls.train_test_split(table, unique_key="key", test_size=1.5)
    with pytest.raises(ValueError, match="test size should be a float between 0 and 1"):
        ls.train_test_split(table, unique_key="key", test_size=-0.5)


def test_train_test_split_invalid_num_buckets_type():
    table = memtable({"key": [1, 2, 3]})
    with pytest.raises(ValueError, match="num_buckets must be an integer"):
        ls.train_test_split(table, unique_key="key", test_size=0.5, num_buckets=10.5)


def test_train_test_split_invalid_num_buckets_value():
    table = memtable({"key": [1, 2, 3]})
    with pytest.raises(
        ValueError, match="num_buckets = 1 places all data into training set"
    ):
        ls.train_test_split(table, unique_key="key", test_size=0.5, num_buckets=1)


def test_train_test_split_multiple_keys():
    data = {
        "key1": range(100),
        "key2": [chr(i % 26 + 65) for i in range(100)],  # A, B, C, ...
        "value": [i % 3 for i in range(100)],
    }
    table = memtable(data)
    train_table, test_table = ls.train_test_split(
        table,
        unique_key=["key1", "key2"],
        test_size=0.25,
        num_buckets=10,
        random_seed=99,
    )
    assert train_table.union(test_table).join(table, how="anti").count().execute() == 0


def test_train_test_splits_valid_split_sizes():
    N = 100000
    table = memtable({"key": range(N), "value": range(N)})
    test_sizes = [0.2, 0.5, 0.8]
    expected_approximate_train_sizes = [(0.78, 0.82), (0.48, 52), (0.18, 0.22)]
    result_splits = ls.train_test_splits(table, "key", test_sizes)
    # get train proportion
    result_ratios = [
        train.count().execute() / (train.count().execute() + test.count().execute())
        for train, test in result_splits
    ]

    for i, result_ratio in enumerate(result_ratios):
        low, up = expected_approximate_train_sizes[i]
        assert low < result_ratio < up


def test_train_test_splits_deterministic_with_seed():
    table = memtable({"key": range(100), "value": range(100)})
    test_sizes = [0.3, 0.6]
    splits1 = list(
        ls.train_test_splits(table, "key", test_sizes, random_seed=123, num_buckets=10)
    )
    splits2 = list(
        ls.train_test_splits(table, "key", test_sizes, random_seed=123, num_buckets=10)
    )

    for (train1, test1), (train2, test2) in zip(splits1, splits2):
        assert train1.count().execute() == train2.count().execute()
        assert test1.count().execute() == test2.count().execute()


def test_train_test_splits_invalid_test_sizes():
    table = memtable({"key": [1, 2, 3], "value": [4, 5, 6]})
    with pytest.raises(ValueError, match="test_sizes needs to be a list of floats."):
        next(ls.train_test_splits(table, "key", ["a", "b"]))
    with pytest.raises(
        ValueError, match="test size should be a float between 0 and 1."
    ):
        next(ls.train_test_splits(table, "key", [-0.1, 0.5]))
