from pathlib import Path

import xorq as xq
from xorq.common.caching import ParquetCacheStorage


t = xq.examples.penguins.fetch(deferred=False)
con = t.op().source
storage = ParquetCacheStorage(source=con, path=Path.cwd())

cached = t.filter([t.species == "Adelie"]).cache(storage=storage)
(op,) = cached.ls.cached_nodes
path = storage.get_loc(op.to_expr().ls.get_key())
print(f"{path} exists?: {path.exists()}")
result = xq.execute(cached)
print(f"{path} exists?: {path.exists()}")
