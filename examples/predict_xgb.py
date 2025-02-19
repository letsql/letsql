import tempfile

import xorq as xq
from xorq.common.caching import ParquetCacheStorage


model_name = "diamonds-model"
model_path = xq.options.pins.get_path(model_name)

# change tmp_dir to the desired directory
with tempfile.TemporaryDirectory() as tmp_dir:
    con = xq.connect()
    predict_xgb = con.register_xgb_model(model_name, model_path)
    cache = ParquetCacheStorage(source=con, path=tmp_dir)
    pg = xq.postgres.connect_examples()
    t = pg.table("diamonds").cache(storage=cache)

    output = t.mutate(prediction=predict_xgb.on_expr).pipe(xq.execute)
    print(output)
