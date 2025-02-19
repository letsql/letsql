from pathlib import Path

import pandas as pd

import xorq as xq
from xorq.expr.ml import make_quickgrove_udf, rewrite_quickgrove_expr
from xorq.expr.relations import into_backend


model_path = Path(xq.options.pins.get_path("diamonds-model"))

# source backend
pg = xq.postgres.connect_examples()
# local backend
con = xq.connect()
# load xgboost json model
# cannot have hyphen in the model name see #498
model = make_quickgrove_udf(model_path, model_name="diamonds_model")

t = (
    into_backend(pg.tables["diamonds"], con, "diamonds")
    .mutate(pred=model.on_expr)
    .filter(xq._.carat < 1)
    .select(xq._.pred)
)

t_pruned = rewrite_quickgrove_expr(t)

original = xq.execute(t)
pruned = xq.execute(t_pruned)

pd.testing.assert_frame_equal(original, pruned, rtol=3)
