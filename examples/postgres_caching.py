import xorq as xq
from xorq import _
from xorq.common.caching import ParquetCacheStorage


pg = xq.postgres.connect_examples()
con = xq.connect()

for table_name in pg.list_tables():
    if table_name.startswith(xq.config.options.cache.key_prefix):
        pg.drop_table(table_name)

cache = ParquetCacheStorage(source=con)

t = (
    pg.table("batting")
    .mutate(row_number=xq.row_number().over(group_by=[_.playerID], order_by=[_.yearID]))
    .filter(_.row_number == 1)
    .cache(storage=cache)
)
print(f"{t.ls.get_key()} exists?: {t.ls.exists()}")
res = xq.execute(t)
print(res)
print(f"{t.ls.get_key()} exists?: {t.ls.exists()}")
