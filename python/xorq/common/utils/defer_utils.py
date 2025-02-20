import pandas as pd
import pyarrow as pa
import toolz

import xorq as xo
from xorq.common.utils.inspect_utils import (
    get_arguments,
)
from xorq.expr.relations import (
    Read,
)
from xorq.vendor import ibis
from xorq.vendor.ibis.util import (
    gen_name,
)


DEFAULT_CHUNKSIZE = 10_000


def make_read_kwargs(f, *args, **kwargs):
    # FIXME: if any kwarg is a dictionary, we'll fail Concrete's hashable requirement, so just pickle
    read_kwargs = get_arguments(f, *args, **kwargs)
    kwargs = read_kwargs.pop("kwargs", {})
    tpl = tuple(read_kwargs.items()) + tuple(kwargs.items())
    return tpl


@toolz.curry
def infer_csv_schema_pandas(path, chunksize=DEFAULT_CHUNKSIZE, **kwargs):
    gen = pd.read_csv(path, chunksize=chunksize, **kwargs)
    df = next(gen)
    batch = pa.RecordBatch.from_pandas(df)
    schema = ibis.Schema.from_pyarrow(batch.schema)
    return schema


def read_csv_rbr(*args, schema=None, chunksize=DEFAULT_CHUNKSIZE, dtype=None, **kwargs):
    """Deferred and streaming csv reading via pandas"""
    if dtype is not None:
        raise Exception("pass `dtype` as pyarrow `schema`")
    if chunksize is None:
        raise ValueError("chunksize must not be `None`")
    if schema is not None:
        dtype = {col: typ.to_pandas() for col, typ in schema.items()}
        schema = schema.to_pyarrow()
    # schema is always nullable (this is good)
    gen = map(
        pa.RecordBatch.from_pandas,
        pd.read_csv(
            *args,
            dtype=dtype,
            chunksize=chunksize,
            **kwargs,
        ),
    )
    if schema is None:
        (el, gen) = toolz.peek(gen)
        schema = el.schema
    rbr = pa.RecordBatchReader.from_batches(
        schema,
        gen,
    )
    return rbr


def deferred_read_csv(con, path, table_name=None, schema=None, **kwargs):
    infer_schema = kwargs.pop("infer_schema", infer_csv_schema_pandas)
    deferred_read_csv.method_name = method_name = "read_csv"
    method = getattr(con, method_name)
    if table_name is None:
        table_name = gen_name(f"xorq-{method_name}")
    if schema is None:
        schema = infer_schema(path)
    if con.name == "pandas":
        # FIXME: determine how to best handle schema
        read_kwargs = make_read_kwargs(method, path, table_name, **kwargs)
    elif con.name == "duckdb":
        read_kwargs = make_read_kwargs(method, path, table_name, types=schema, **kwargs)
    else:
        read_kwargs = make_read_kwargs(
            method, path, table_name, schema=schema, **kwargs
        )
    return Read(
        method_name=method_name,
        name=table_name,
        schema=schema,
        source=con,
        read_kwargs=read_kwargs,
    ).to_expr()


def deferred_read_parquet(con, path, table_name=None, **kwargs):
    deferred_read_parquet.method_name = method_name = "read_parquet"
    method = getattr(con, method_name)
    if table_name is None:
        table_name = gen_name(f"letsql-{method_name}")
    schema = xo.connect().read_parquet(path).schema()
    read_kwargs = make_read_kwargs(method, path, table_name, **kwargs)
    return Read(
        method_name=method_name,
        name=table_name,
        schema=schema,
        source=con,
        read_kwargs=read_kwargs,
    ).to_expr()


def rbr_wrapper(reader, clean_up):
    def gen():
        yield from reader
        clean_up()

    return pa.RecordBatchReader.from_batches(reader.schema, gen())
