import xorq as xo
from xorq.expr.relations import into_backend


pg = xo.postgres.connect_env()
db = xo.duckdb.connect()

batting = pg.table("batting")

awards_players = xo.examples.awards_players.fetch(backend=db)
left = batting.filter(batting.yearID == 2015)
right = awards_players.filter(awards_players.lgID == "NL").drop("yearID", "lgID")
expr = left.join(into_backend(right, pg), ["playerID"], how="semi")[["yearID", "stint"]]


result = xo.execute(expr)
print(result)
