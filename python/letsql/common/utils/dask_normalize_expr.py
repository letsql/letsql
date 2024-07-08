import re
import types

import dask
import ibis
import ibis.expr.operations.relations as ir
import sqlglot as sg
from ibis.expr.operations.udf import (
    AggUDF,
    ScalarUDF,
)

import letsql
from letsql.expr.relations import (
    make_native_op,
)


def expr_is_bound(expr):
    backends, _ = expr._find_backends()
    return bool(backends)


def unbound_expr_to_default_sql(expr):
    if expr_is_bound(expr):
        raise ValueError
    default_sql = letsql.to_sql(expr)
    return str(default_sql)


def normalize_memory_databasetable(dt):
    if dt.source.name not in ("pandas", "let", "datafusion", "duckdb"):
        raise ValueError
    return dask.base._normalize_seq_func(
        (
            # we are normalizing the data, we don't care about the connection
            # dt.source,
            dt.schema.to_pandas(),
            # in memory: so we can assume its reasonable to hash the data
            tuple(
                dask.base.tokenize(el.serialize().to_pybytes())
                for el in dt.to_expr().to_pyarrow_batches()
            ),
        )
    )


def normalize_pandas_databasetable(dt):
    if dt.source.name != "pandas":
        raise ValueError
    return normalize_memory_databasetable(dt)


def normalize_datafusion_databasetable(dt):
    if dt.source.name not in ("datafusion", "let"):
        raise ValueError
    ep_str = str(dt.source.con.table(dt.name).execution_plan())
    if ep_str.startswith(("ParquetExec:", "CsvExec:")):
        return dask.base._normalize_seq_func(
            (
                dt.schema.to_pandas(),
                # ep_str denotes the parquet files to be read
                # FIXME: md5sum on detected .parquet files?
                ep_str,
            )
        )
    elif ep_str.startswith("MemoryExec:"):
        return normalize_memory_databasetable(dt)
    else:
        raise ValueError


def normalize_remote_databasetable(dt):
    return dask.base._normalize_seq_func(
        (
            dt.name,
            dt.schema,
            dt.source,
            dt.namespace,
        )
    )


def normalize_postgres_databasetable(dt):
    from letsql.common.utils.postgres_utils import get_postgres_n_reltuples

    if dt.source.name != "postgres":
        raise ValueError
    return dask.base._normalize_seq_func(
        (
            dt.name,
            dt.schema,
            dt.source,
            dt.namespace,
            get_postgres_n_reltuples(dt),
        )
    )


def normalize_snowflake_databasetable(dt):
    from letsql.common.utils.snowflake_utils import get_snowflake_last_modification_time

    if dt.source.name != "snowflake":
        raise ValueError
    return dask.base._normalize_seq_func(
        (
            dt.name,
            dt.schema,
            dt.source,
            dt.namespace,
            get_snowflake_last_modification_time(dt),
        )
    )


def normalize_duckdb_databasetable(dt):
    if dt.source.name != "duckdb":
        raise ValueError
    name = sg.table(dt.name, quoted=dt.source.compiler.quoted).sql(
        dialect=dt.source.name
    )
    ((_, plan),) = dt.source.raw_sql(f"EXPLAIN SELECT * FROM {name}").fetchall()
    scan_line = plan.split("\n")[1]
    execution_plan_name = r"\s*│\s*(\w+)\s*│\s*"
    match re.match(execution_plan_name, scan_line).group(1):
        case "ARROW_SCAN":
            return normalize_memory_databasetable(dt)
        case "READ_PARQUET" | "READ_CSV" | "SEQ_SCAN":
            return normalize_duckdb_file_read(dt)
        case _:
            raise NotImplementedError(scan_line)


def normalize_duckdb_file_read(dt):
    name = sg.exp.convert(dt.name).sql(dialect=dt.source.name)
    (sql_ddl_statement,) = dt.source.con.sql(
        f"select sql from duckdb_views() where view_name = {name} UNION select sql from duckdb_tables() where table_name = {name}"
    ).fetchone()
    return dask.base._normalize_seq_func(
        (
            dt.schema.to_pandas(),
            # sql_ddl_statement denotes the definition of the table, expressed as SQL DDL-statement.
            sql_ddl_statement,
        )
    )


def normalize_letsql_databasetable(dt):
    if dt.source.name != "let":
        raise ValueError
    native_source = dt.source._sources.get_backend(dt)
    if native_source.name == "let":
        return normalize_datafusion_databasetable(dt)
    new_dt = make_native_op(dt)
    return dask.base.normalize_token(new_dt)


@dask.base.normalize_token.register(types.ModuleType)
def normalize_module(module):
    return dask.base._normalize_seq_func(
        (
            module.__name__,
            module.__package__,
        )
    )


@dask.base.normalize_token.register(ir.DatabaseTable)
def normalize_databasetable(dt):
    dct = {
        "pandas": normalize_pandas_databasetable,
        "datafusion": normalize_datafusion_databasetable,
        "postgres": normalize_postgres_databasetable,
        "snowflake": normalize_snowflake_databasetable,
        "let": normalize_letsql_databasetable,
        "duckdb": normalize_duckdb_databasetable,
    }
    f = dct[dt.source.name]
    return f(dt)


@dask.base.normalize_token.register(ibis.backends.BaseBackend)
def normalize_backend(con):
    name = con.name
    if name == "snowflake":
        con_details = con.con._host
    elif name == "postgres":
        con_dct = con.con.get_dsn_parameters()
        con_details = {k: con_dct[k] for k in ("host", "port", "dbname")}
    elif name == "pandas":
        con_details = id(con.dictionary)
    elif name in ("datafusion", "duckdb", "let"):
        con_details = id(con.con)
    else:
        raise ValueError
    return (name, con_details)


@dask.base.normalize_token.register(ir.Schema)
def normalize_schema(schema):
    return dask.base._normalize_seq_func((schema.to_pandas(),))


@dask.base.normalize_token.register(ir.Namespace)
def normalize_namespace(ns):
    return dask.base._normalize_seq_func(
        (
            ns.catalog,
            ns.database,
        )
    )


@dask.base.normalize_token.register(ScalarUDF)
def normalize_scalar_udf(udf):
    typs = tuple(arg.dtype for arg in udf.args)
    return dask.base._normalize_seq_func(
        (
            ScalarUDF,
            typs,
            udf.dtype,
            udf.__func__,
            # we are insensitive to these for now
            # udf.__udf_namespace__,
            # udf.__func_name__,
        )
    )


@dask.base.normalize_token.register(AggUDF)
def normalize_agg_udf(udf):
    (*args, where) = udf.args
    if where is not None:
        # TODO: determine if sql string already contains
        #       the relevant information of `where`
        raise NotImplementedError
    typs = tuple(arg.dtype for arg in args)
    return dask.base._normalize_seq_func(
        (
            AggUDF,
            typs,
            udf.dtype,
            udf.__func__,
            # we are insensitive to these for now
            # udf.__udf_namespace__,
            # udf.__func_name__,
        )
    )


@dask.base.normalize_token.register(ibis.expr.types.Expr)
def normalize_expr(expr):
    # FIXME: replace bound table names with their hashes
    sql = unbound_expr_to_default_sql(expr.ls.uncached.unbind())
    if not expr_is_bound(expr):
        return sql

    op = expr.op()
    if mem_dts := op.find(ir.InMemoryTable):
        # these should have been replaced by the time we get to them
        raise ValueError(f"{mem_dts}")
    dts = op.find(ir.DatabaseTable)
    udfs = op.find((AggUDF, ScalarUDF))
    return dask.base._normalize_seq_func(
        (
            sql,
            dts,
            udfs,
        )
    )
