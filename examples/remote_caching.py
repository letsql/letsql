import xorq as xq
from xorq import _
from xorq.common.caching import SourceStorage


con = xq.connect()
ddb = xq.duckdb.connect()
pg = xq.postgres.connect_env()

name = "batting"

right = (
    xq.examples.get_table_from_name(name, backend=ddb)
    .filter(_.yearID == 2014)
    .into_backend(con)
)
left = pg.table(name).filter(_.yearID == 2015).into_backend(con)

expr = left.join(
    right,
    "playerID",
).cache(SourceStorage(source=pg))

res = expr.execute()
print(res)
