import ibis
import letsql as ls


table_name = "iris"
source = ls.config.options.pins.get_path(table_name)
schema = ls.connect().read_csv(source).schema()


pd_con_before = ibis.options.default_backend = ibis.pandas.connect()
baseline_before = pd_con_before.read_csv(source).sepal_length.max()


pd_con_after = ls.pandas.connect()
baseline_after = pd_con_before.read_csv(source).sepal_length.max()
without_schema = pd_con_after.read_csv(source, table_name).sepal_length.max()
with_schema = pd_con_after.read_csv(
    source, table_name, schema=schema
).sepal_length.max()


baseline_before.execute()
without_schema.execute()
with_schema.execute()


assert "Read" not in repr(baseline_before)
assert "Read" in repr(baseline_after)
assert "Read" in repr(without_schema)
assert "Read" in repr(with_schema)
