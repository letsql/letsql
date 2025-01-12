import letsql as ls
from letsql import memtable

N = 100000
# if single float deferred partitions of train and test will be returned
# With proportions (1-test_size, test_size)
test_size = 0.25
# init table
table = memtable([(i, "val") for i in range(N)], columns=["key1", "val"])


train_table, test_table = ls.train_test_splits(
    table, unique_key="key1", test_sizes=test_size, num_buckets=N, random_seed=42
)

train_count = train_table.count().execute()
test_count = test_table.count().execute()
total = train_count + test_count
print(f"train ratio: {train_count/total}")
print(f"test ratio: {test_count/total}\n")


# If test sizes is a list of floats , mutually exclusive partitions will be returned
partition_proportions = [0.1, 0.2, 0.3, 0.4]


hold_out, test, validation, training = ls.train_test_splits(
    table,
    unique_key="key1",
    test_sizes=partition_proportions,
    num_buckets=N,
    random_seed=42,
)

partitions = [hold_out, test, validation, training]
total = (
    hold_out.count().execute()
    + test.count().execute()
    + validation.count().execute()
    + training.count().execute()
)
for partition in partitions:
    print(f"Partition Ratio:{partition.count().execute()/ total}")
