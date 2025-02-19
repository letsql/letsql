import xorq as xq
from xorq.common.caching import SourceStorage


con = xq.connect()
pg = xq.postgres.connect_env()

t = pg.table("batting").into_backend(con, "ls_batting")

expr = (
    t.join(t, "playerID")
    .limit(15)
    .select(player_id="playerID", year_id="yearID_right")
    .cache(SourceStorage(source=con))
)

print(expr.execute())
print(con.list_tables())
