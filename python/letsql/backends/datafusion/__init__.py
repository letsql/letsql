from __future__ import annotations

import contextlib
import functools
import inspect
import types
import typing
import toolz
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ibis
from ibis.common.annotations import Argument
from ibis.expr.operations import Namespace
import ibis.common.exceptions as com
import ibis.expr.rules as rlz
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import json
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow_hotfix  # noqa: F401
import sqlglot as sg
import sqlglot.expressions as sge
from ibis.backends import CanCreateCatalog, CanCreateDatabase, CanCreateSchema, NoUrl
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compiler import C
from ibis.expr.operations.udf import InputType
from ibis.formats.pyarrow import PyArrowType
from ibis.util import gen_name, normalize_filename
from ibis.expr.operations.udf import ScalarUDF

import letsql
import letsql.internal as df
from letsql.backends.datafusion.compiler import DataFusionCompiler
from letsql.backends.datafusion.provider import IbisTableProvider
from letsql.expr.pyaggregator import PyAggregator, make_struct_type
from letsql.internal import (
    SessionConfig,
    SessionContext,
    TableProvider,
    Table,
    DataFrame,
)

if TYPE_CHECKING:
    import pandas as pd


def _compile_pyarrow_udaf(udaf_node):
    func = udaf_node.__func__
    name = type(udaf_node).__name__
    return_type = PyArrowType.from_ibis(udaf_node.dtype)
    parameters = (
        (name, PyArrowType.from_ibis(param.annotation.pattern.dtype))
        for name, param in udaf_node.__signature__.parameters.items()
        if name != "where"
    )
    names, input_types = map(list, zip(*parameters))  # noqa
    struct_type = make_struct_type(names, input_types)

    class MyAggregator(PyAggregator):
        @classmethod
        @property
        def struct_type(cls):
            return struct_type

        def py_evaluate(self):
            struct_array = self.pystate()
            args = (struct_array.field(field_name) for field_name in self.names)
            return func(*args)

        @classmethod
        @property
        def return_type(cls):
            return return_type

        @classmethod
        @property
        def name(cls):
            return name

    return df.udaf(
        accum=MyAggregator,
        input_type=input_types,
        return_type=return_type,
        state_type=[MyAggregator.state_type],
        volatility=MyAggregator.volatility,
        name=name,
    )


def _inspect_xgboost_model_from_json(json_file_path):
    with open(json_file_path, "r") as file:
        model_data = json.load(file)

    learner_data = model_data["learner"]
    model_attributes = learner_data["attributes"]
    feature_names = learner_data["feature_names"]
    feature_types = learner_data["feature_types"]
    gbtree_model_param = learner_data["gradient_booster"]["model"]["gbtree_model_param"]
    num_trees = gbtree_model_param["num_trees"]

    metadata = {
        "model_attributes": model_attributes,
        "feature_names": feature_names,
        "feature_types": feature_types,
        "gbtree_model_param": gbtree_model_param,
        "number_of_trees": num_trees,
    }

    return metadata


def _fields_to_parameters(fields):
    parameters = []
    for name, arg in fields.items():
        param = inspect.Parameter(
            name, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=arg.typehint
        )
        parameters.append(param)
    return parameters


class Backend(SQLBackend, CanCreateCatalog, CanCreateDatabase, CanCreateSchema, NoUrl):
    name = "datafusion"
    supports_in_memory_tables = True
    supports_arrays = True
    compiler = DataFusionCompiler()

    @property
    def version(self):
        return letsql.__version__

    def do_connect(self, config: Mapping[str, str | Path] | None = None) -> None:
        df_config = SessionConfig(
            {"datafusion.sql_parser.dialect": "PostgreSQL"}
        ).with_information_schema(True)
        self.con = SessionContext(config=df_config)

        self._register_builtin_udfs()

        if not config:
            config = {}

        for name, path in config.items():
            self.register(path, table_name=name)

    def disconnect(self) -> None:
        pass

    @contextlib.contextmanager
    def _safe_raw_sql(self, sql: sge.Statement) -> Any:
        yield self.raw_sql(sql).collect()

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        name = gen_name("datafusion_metadata_view")
        table = sg.table(name, quoted=self.compiler.quoted)
        src = sge.Create(
            this=table,
            kind="VIEW",
            expression=sg.parse_one(query, read="datafusion"),
            properties=sge.Properties(expressions=[sge.TemporaryProperty()]),
        )

        with self._safe_raw_sql(src):
            pass

        try:
            result = (
                self.raw_sql(f"DESCRIBE {table.sql(self.dialect)}")
                .to_arrow_table()
                .to_pydict()
            )
        finally:
            self.drop_view(name)
        return sch.Schema(
            {
                name: self.compiler.type_mapper.from_string(
                    type_string, nullable=is_nullable == "YES"
                )
                for name, type_string, is_nullable in zip(
                    result["column_name"], result["data_type"], result["is_nullable"]
                )
            }
        )

    def _register_builtin_udfs(self):
        from letsql.backends.datafusion import udfs

        for name, func in inspect.getmembers(
            udfs,
            predicate=lambda m: callable(m)
            and not m.__name__.startswith("_")
            and m.__module__ == udfs.__name__,
        ):
            annotations = typing.get_type_hints(func)
            argnames = list(inspect.signature(func).parameters.keys())
            input_types = [
                PyArrowType.from_ibis(dt.dtype(annotations.get(arg_name)))
                for arg_name in argnames
            ]
            return_type = PyArrowType.from_ibis(dt.dtype(annotations["return"]))
            udf = df.udf(
                func,
                input_types=input_types,
                return_type=return_type,
                volatility="immutable",
                name=name,
            )
            self.con.register_udf(udf)

    def _register_udfs(self, expr: ir.Expr) -> None:
        for udf_node in expr.op().find(ops.ScalarUDF):
            if udf_node.__input_type__ == InputType.PYARROW:
                udf = self._compile_pyarrow_udf(udf_node)
                self.con.register_udf(udf)

        for udf_node in expr.op().find(ops.ElementWiseVectorizedUDF):
            udf = self._compile_elementwise_udf(udf_node)
            self.con.register_udf(udf)

        for udaf_node in expr.op().find(ops.AggUDF):
            if udaf_node.__input_type__ == InputType.PYARROW:
                udaf = _compile_pyarrow_udaf(udaf_node)
                self.con.register_udaf(udaf)

    def _compile_pyarrow_udf(self, udf_node):
        return df.udf(
            udf_node.__func__,
            input_types=[PyArrowType.from_ibis(arg.dtype) for arg in udf_node.args],
            return_type=PyArrowType.from_ibis(udf_node.dtype),
            volatility=getattr(udf_node, "__config__", {}).get(
                "volatility", "volatile"
            ),
            name=udf_node.__func_name__,
        )

    def _compile_elementwise_udf(self, udf_node):
        return df.udf(
            udf_node.func,
            input_types=list(map(PyArrowType.from_ibis, udf_node.input_type)),
            return_type=PyArrowType.from_ibis(udf_node.return_type),
            volatility="volatile",
            name=udf_node.func.__name__,
        )

    def raw_sql(self, query: str | sge.Expression) -> Any:
        """Execute a SQL string `query` against the database.

        Parameters
        ----------
        query
            Raw SQL string
        """
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        self._log(query)
        return self.con.sql(query)

    @property
    def current_catalog(self) -> str:
        raise NotImplementedError()

    @property
    def current_database(self) -> str:
        raise NotImplementedError()

    def list_catalogs(self, like: str | None = None) -> list[str]:
        code = (
            sg.select(C.table_catalog)
            .from_(sg.table("tables", db="information_schema"))
            .distinct()
        ).sql()
        result = self.con.sql(code).to_pydict()
        return self._filter_with_like(result["table_catalog"], like)

    def create_catalog(self, name: str, force: bool = False) -> None:
        with self._safe_raw_sql(
            sge.Create(kind="DATABASE", this=sg.to_identifier(name), exists=force)
        ):
            pass

    def drop_catalog(self, name: str, force: bool = False) -> None:
        raise com.UnsupportedOperationError(
            "DataFusion does not support dropping databases"
        )

    def list_databases(
        self, like: str | None = None, catalog: str | None = None
    ) -> list[str]:
        return self._filter_with_like(
            self.con.catalog(catalog if catalog is not None else "datafusion").names(),
            like=like,
        )

    def create_database(
        self, name: str, catalog: str | None = None, force: bool = False
    ) -> None:
        # not actually a table, but this is how sqlglot represents schema names
        db_name = sg.table(name, db=catalog)
        with self._safe_raw_sql(sge.Create(kind="SCHEMA", this=db_name, exists=force)):
            pass

    def drop_database(
        self, name: str, catalog: str | None = None, force: bool = False
    ) -> None:
        db_name = sg.table(name, db=catalog)
        with self._safe_raw_sql(sge.Drop(kind="SCHEMA", this=db_name, exists=force)):
            pass

    def list_tables(
        self,
        like: str | None = None,
        database: str | None = None,
    ) -> list[str]:
        """Return the list of table names in the current database.

        Parameters
        ----------
        like
            A pattern in Python's regex format.
        database
            Unused in the datafusion backend.

        Returns
        -------
        list[str]
            The list of the table names that match the pattern `like`.
        """
        return self._filter_with_like(self.con.tables(), like)

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        if catalog is not None:
            catalog = self.con.catalog(catalog)
        else:
            catalog = self.con.catalog()

        if database is not None:
            database = catalog.database(database)
        else:
            database = catalog.database()

        table = database.table(table_name)
        return sch.schema(table.schema)

    def register(
        self,
        source: str | Path | pa.Table | pa.RecordBatch | pa.Dataset | pd.DataFrame,
        table_name: str | None = None,
        **kwargs: Any,
    ) -> ir.Table:
        """Register a data set with `table_name` located at `source`.

        Parameters
        ----------
        source
            The data source(s). Maybe a path to a file or directory of
            parquet/csv files, a pandas dataframe, or a pyarrow table, dataset
            or record batch.
        table_name
            The name of the table
        kwargs
            Datafusion-specific keyword arguments

        """
        import pandas as pd

        quoted = self.compiler.quoted
        table_ident = str(sg.to_identifier(table_name, quoted=quoted))

        if isinstance(source, (str, Path)):
            first = str(source)
        elif isinstance(source, pa.Table):
            self.con.deregister_table(table_ident)
            self.con.register_record_batches(table_ident, [source.to_batches()])
            return self.table(table_name)
        elif isinstance(source, pa.RecordBatch):
            self.con.deregister_table(table_ident)
            self.con.register_record_batches(table_ident, [[source]])
            return self.table(table_name)
        elif isinstance(source, pa.dataset.Dataset):
            self.con.deregister_table(table_ident)
            self.con.register_dataset(table_ident, source)
            return self.table(table_name)
        elif isinstance(source, pd.DataFrame):
            output = pa.Table.from_pandas(source)
            output = output.drop(
                filter(
                    lambda col: col.startswith("__index_level_"), output.column_names
                )
            )
            return self.register(output, table_name, **kwargs)
        elif isinstance(source, ibis.expr.types.Table) and hasattr(
            source, "to_pyarrow_batches"
        ):
            self.con.deregister_table(table_ident)
            self.con.register_table_provider(
                table_ident, TableProvider(IbisTableProvider(source))
            )
            return self.table(table_name)
        elif isinstance(source, ibis.expr.types.Expr) and hasattr(
            source, "to_pyarrow_batches"
        ):
            self.con.deregister_table(table_ident)
            self.con.register_record_batch_reader(
                table_ident, source.to_pyarrow_batches()
            )
            return self.table(table_name)
        elif isinstance(source, pa.RecordBatchReader):
            self.con.deregister_table(table_ident)
            self.con.register_record_batch_reader(table_ident, source)
            return self.table(table_name)
        elif isinstance(source, Table):
            self.con.deregister_table(table_ident)
            self.con.register_table(table_ident, source)
            return self.table(table_name)
        elif isinstance(source, DataFrame):
            self.con.deregister_table(table_ident)
            self.con.register_dataframe(table_ident, source)
            return self.table(table_name)
        else:
            raise ValueError(f"Unknown `source` type {type(source)}")

        if first.startswith(("parquet://", "parq://")) or first.endswith(
            ("parq", "parquet")
        ):
            return self.read_parquet(source, table_name=table_name, **kwargs)
        elif first.startswith(("csv://", "txt://")) or first.endswith(
            ("csv", "tsv", "txt")
        ):
            return self.read_csv(source, table_name=table_name, **kwargs)
        else:
            self._register_failure()
            return None

    def register_table_provider(
        self,
        source: str | Path | pa.Table | pa.RecordBatch | pa.Dataset | pd.DataFrame,
        table_name: str | None = None,
    ):
        table_ident = str(sg.to_identifier(table_name, quoted=self.compiler.quoted))
        self.con.deregister_table(table_ident)
        self.con.register_table_provider(
            table_ident, TableProvider(IbisTableProvider(source))
        )
        return self.table(table_name)

    def _register_failure(self):
        import inspect

        msg = ", ".join(
            m[0] for m in inspect.getmembers(self) if m[0].startswith("read_")
        )
        raise ValueError(
            f"Cannot infer appropriate read function for input, "
            f"please call one of {msg} directly"
        )

    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        name = op.name
        schema = op.schema

        self.con.deregister_table(name)
        if batches := op.data.to_pyarrow(schema).to_batches():
            self.con.register_record_batches(name, [batches])
        else:
            empty_dataset = ds.dataset([], schema=schema.to_pyarrow())
            self.con.register_dataset(name=name, dataset=empty_dataset)

    def _register_in_memory_tables(self, expr: ir.Expr) -> None:
        if self.supports_in_memory_tables:
            for memtable in expr.op().find(ops.InMemoryTable):
                self._register_in_memory_table(memtable)

    def register_xgb_model(
        self, model_name: str, source: str | Path
    ) -> typing.Callable:
        """Register an XGBoost model as a UDF in the `letsql` Backend.

        Parameters
        ----------
        model_name
            The name of the model
        source
            The path to the JSON file containing the XGBoost model

        Returns
        -------
        typing.Callable
            A function that can be used to call the XGBoost model as an Ibis UDF
        """
        source = str(source)
        metadata = _inspect_xgboost_model_from_json(source)
        self.con.register_xgb_json_model(model_name, source)
        features = metadata["feature_names"]
        feature_types = [
            dt.dtype(feature_type) for feature_type in metadata["feature_types"]
        ]
        return_type = dt.dtype("float64")
        bases = (ScalarUDF,)
        fields = {
            k: v
            for k, v in (
                ("model_name", Argument(pattern=rlz.ValueOf(str), typehint=str)),
            )
            + tuple(
                (arg_name, Argument(pattern=rlz.ValueOf(typ), typehint=typ))
                for (arg_name, typ) in zip(features, feature_types)
            )
        }

        fn = toolz.functoolz.return_none

        meta = {
            "dtype": return_type,
            "__input_type__": InputType.BUILTIN,
            "__func__": property(fget=lambda _, fn=fn: fn),
            "__config__": {"volatility": "immutable"},
            "__udf_namespace__": Namespace(database=None, catalog=None),
            "__module__": self.__module__,
            "__func_name__": "predict_xgb",
        }
        kwds = {
            **fields,
            **meta,
        }

        node = type(
            "predict_xgb",
            bases,
            kwds,
        )

        @functools.wraps(fn)
        def construct(*args: Any, **kwargs: Any) -> ir.Value:
            return node(*args, **kwargs).to_expr()

        partial = toolz.functoolz.partial(construct, model_name)

        def on_expr(expr, *args, **kwargs):
            return partial(*(expr[c] for c in features), *args, **kwargs)

        def create_named_wrapper(func, name, signature):
            def predict_xgb(*args, **kwargs):
                return func(*args, **kwargs)

            new_func = types.FunctionType(
                predict_xgb.__code__,
                predict_xgb.__globals__,
                name=name,
                argdefs=predict_xgb.__defaults__,
                closure=predict_xgb.__closure__,
            )
            new_func.__signature__ = signature
            return new_func

        new_signature = inspect.Signature(_fields_to_parameters(fields)[1:])
        wrapper = create_named_wrapper(partial, model_name, new_signature)
        wrapper.on_expr = on_expr
        return wrapper

    def read_csv(
        self, path: str | Path, table_name: str | None = None, **kwargs: Any
    ) -> ir.Table:
        """Register a CSV file as a table in the current database.

        Parameters
        ----------
        path
            The data source. A string or Path to the CSV file.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.
        **kwargs
            Additional keyword arguments passed to Datafusion loading function.

        Returns
        -------
        ir.Table
            The just-registered table

        """
        path = normalize_filename(path)
        table_name = table_name or gen_name("read_csv")
        # Our other backends support overwriting views / tables when re-registering
        self.con.deregister_table(table_name)
        self.con.register_csv(table_name, path, **kwargs)
        return self.table(table_name)

    def read_parquet(
        self, path: str | Path, table_name: str | None = None, **kwargs: Any
    ) -> ir.Table:
        """Register a parquet file as a table in the current database.

        Parameters
        ----------
        path
            The data source.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.
        **kwargs
            Additional keyword arguments passed to Datafusion loading function.

        Returns
        -------
        ir.Table
            The just-registered table

        """
        path = normalize_filename(path)
        table_name = table_name or gen_name("read_parquet")
        # Our other backends support overwriting views / tables when reregistering
        self.con.deregister_table(table_name)
        self.con.register_parquet(table_name, path, **kwargs)
        return self.table(table_name)

    def read_delta(
        self, source_table: str | Path, table_name: str | None = None, **kwargs: Any
    ) -> ir.Table:
        """Register a Delta Lake table as a table in the current database.

        Parameters
        ----------
        source_table
            The data source. Must be a directory
            containing a Delta Lake table.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.
        **kwargs
            Additional keyword arguments passed to deltalake.DeltaTable.

        Returns
        -------
        ir.Table
            The just-registered table

        """
        source_table = normalize_filename(source_table)

        table_name = table_name or gen_name("read_delta")

        # Our other backends support overwriting views / tables when reregistering
        self.con.deregister_table(table_name)

        try:
            from deltalake import DeltaTable
        except ImportError:
            raise ImportError(
                "The deltalake extra is required to use the "
                "read_delta method. You can install it using pip:\n\n"
                "pip install 'ibis-framework[deltalake]'\n"
            )

        delta_table = DeltaTable(source_table, **kwargs)

        return self.register(delta_table.to_pyarrow_dataset(), table_name=table_name)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        *,
        chunk_size: int = 1_000_000,
        **kwargs: Any,
    ) -> pa.ipc.RecordBatchReader:
        pa = self._import_pyarrow()

        self._register_udfs(expr)
        self._register_in_memory_tables(expr)

        table_expr = expr.as_table()
        raw_sql = self.compile(table_expr, **kwargs)

        frame = self.con.sql(raw_sql)

        schema = table_expr.schema()
        pyarrow_schema = schema.to_pyarrow()
        struct_schema = schema.as_struct().to_pyarrow()

        def make_gen():
            yield from (
                # convert the renamed + cast columns into a record batch
                pa.RecordBatch.from_struct_array(
                    # rename columns to match schema because datafusion lowercases things
                    pa.RecordBatch.from_arrays(
                        batch.to_pyarrow().columns, schema=pyarrow_schema
                    )
                    # cast the struct array to the desired types to work around
                    # https://github.com/apache/arrow-datafusion-python/issues/534
                    .to_struct_array()
                    .cast(struct_schema)
                )
                for batch in frame.execute_stream()
            )

        return pa.ipc.RecordBatchReader.from_batches(
            pyarrow_schema,
            make_gen(),
        )

    def to_pyarrow(self, expr: ir.Expr, **kwargs: Any) -> pa.Table:
        batch_reader = self.to_pyarrow_batches(expr, **kwargs)
        arrow_table = batch_reader.read_all()
        return expr.__pyarrow_result__(arrow_table)

    def execute(self, expr: ir.Expr, **kwargs: Any):
        batch_reader = self.to_pyarrow_batches(expr, **kwargs)
        return expr.__pandas_result__(
            batch_reader.read_pandas(timestamp_as_object=True)
        )

    def create_table(
        self,
        name: str,
        obj: pd.DataFrame | pa.Table | ir.Table | None = None,
        *,
        schema: sch.Schema | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ):
        """Create a table in Datafusion.

        Parameters
        ----------
        name
            Name of the table to create
        obj
            The data with which to populate the table; optional, but at least
            one of `obj` or `schema` must be specified
        schema
            The schema of the table to create; optional, but at least one of
            `obj` or `schema` must be specified
        database
            The name of the database in which to create the table; if not
            passed, the current database is used.
        temp
            Create a temporary table
        overwrite
            If `True`, replace the table if it already exists, otherwise fail
            if the table exists

        """
        if obj is None and schema is None:
            raise ValueError("Either `obj` or `schema` must be specified")

        properties = []

        if temp:
            properties.append(sge.TemporaryProperty())

        quoted = self.compiler.quoted

        if obj is not None:
            if not isinstance(obj, ir.Expr):
                table = letsql.memtable(obj)
            else:
                table = obj

            self._run_pre_execute_hooks(table)

            relname = "_"
            query = sg.select(
                *(
                    self.compiler.cast(
                        sg.column(col, table=relname, quoted=quoted), dtype
                    ).as_(col, quoted=quoted)
                    for col, dtype in table.schema().items()
                )
            ).from_(
                self._to_sqlglot(table).subquery(
                    sg.to_identifier(relname, quoted=quoted)
                )
            )
        else:
            query = None

        table_ident = sg.to_identifier(name, quoted=quoted)

        if query is None:
            column_defs = [
                sge.ColumnDef(
                    this=sg.to_identifier(colname, quoted=quoted),
                    kind=self.compiler.type_mapper.from_ibis(typ),
                    constraints=(
                        None
                        if typ.nullable
                        else [sge.ColumnConstraint(kind=sge.NotNullColumnConstraint())]
                    ),
                )
                for colname, typ in (schema or table.schema()).items()
            ]

            target = sge.Schema(this=table_ident, expressions=column_defs)
        else:
            target = table_ident

        create_stmt = sge.Create(
            kind="TABLE",
            this=target,
            properties=sge.Properties(expressions=properties),
            expression=query,
            replace=overwrite,
        )

        with self._safe_raw_sql(create_stmt):
            pass

        return self.table(name, database=database)

    def truncate_table(
        self, name: str, database: str | None = None, schema: str | None = None
    ) -> None:
        """Delete all rows from a table.

        Parameters
        ----------
        name
            Table name
        database
            Database name
        schema
            Schema name

        """
        # datafusion doesn't support `TRUNCATE TABLE` so we use `DELETE FROM`
        #
        # however datafusion as of 34.0.0 doesn't implement DELETE DML yet
        table_loc = self._warn_and_create_table_loc(database, schema)
        catalog, db = self._to_catalog_db_tuple(table_loc)

        ident = sg.table(name, db=db, catalog=catalog).sql(self.name)
        with self._safe_raw_sql(sge.delete(ident)):
            pass
