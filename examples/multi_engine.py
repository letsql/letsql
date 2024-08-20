import letsql as ls

# this example also shows the no need for registering tables or expression directly in LETSQL

pg = ls.postgres.connect_examples()
db = ls.duckdb.connect()


batting = pg.table("batting")
awards_players = db.register(
    ls.config.options.pins.get_path("awards_players"),
    table_name="awards_players",
)
left = batting[batting.yearID == 2015]
right = awards_players[awards_players.lgID == "NL"].drop("yearID", "lgID")
expr = left.join(right, ["playerID"], how="semi")[["yearID", "stint"]]


result = expr.execute()
print(result)
result = expr.to_pyarrow()
print(result)
result = expr.to_pyarrow_batches().read_pandas()
print(result)
print(tuple(dt.args for dt in expr.ls.native_dts))
