import ibis
import letsql as ls
from letsql.common.caching import ParquetCacheStorage
from letsql.common.utils.defer_utils import (
    infer_csv_schema_pandas,
    override_schema,
)


url = "https://opendata-downloads.s3.amazonaws.com/opa_properties_public.csv"


# unfortunately schema inference can be costly
# # currently we use functools.cache
# # consider durable pickle-cache based on mtime
inferred_schema = infer_csv_schema_pandas(url)
# schema inference doesn't catch some tricksy fields
partial_schema = ibis.schema(
    {
        "book_and_page": str,
        "exterior_condition": str,
        "house_extension": str,
        "interior_condition": str,
    }
)
schema = override_schema(
    inferred_schema,
    partial_schema,
)


con = ls.connect()
storage = ParquetCacheStorage(path=".")


t = con.read_csv_pandas(url, table_name="t", schema=schema)
print(t.op().name)
expr = t.cache(storage=storage)
print(storage.get_key(t))
print(storage.exists(t))
# we got a partial write when we failed in the middle
print(expr.count().execute())
