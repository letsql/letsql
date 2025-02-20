from pathlib import Path

import pandas as pd

import xorq as xo
from xorq.expr.ml import make_quickgrove_udf, rewrite_quickgrove_expr
from xorq.expr.relations import into_backend


model_path = Path(xo.options.pins.get_path("diamonds-model"))

# source backend
pg = xo.postgres.connect_examples()
# local backend
con = xo.connect()
# load xgboost json model
# cannot have hyphen in the model name see #498
model = make_quickgrove_udf(model_path, model_name="diamonds_model")

t = (
    into_backend(pg.tables["diamonds"], con, "diamonds")
    .mutate(pred=model.on_expr)
    .filter(xo._.carat < 1)
    .select(xo._.pred)
)

t_pruned = rewrite_quickgrove_expr(t)

original = xo.execute(t)
pruned = xo.execute(t_pruned)

pd.testing.assert_frame_equal(original, pruned, rtol=3)
