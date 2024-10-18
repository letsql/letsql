import pathlib
from typing import Any

import ibis.backends.pandas
import ibis.backends.pandas.executor as IPE
from ibis import util

import letsql as ls
import letsql.backends.pandas.executor as LPE
from letsql.common.utils.hotfix_utils import maybe_hotfix
from letsql.expr.relations import Read


IPE.PandasExecutor = LPE.PandasExecutor


def make_reader_hotfix(cls, methodname):
    original_method = getattr(cls, methodname)

    def reader_hotfix(
        self, source: str | pathlib.Path, table_name: str | None = None, **kwargs: Any
    ):
        schema = kwargs.pop("schema", None)
        if schema is None:
            schema_inference = kwargs.pop("schema_inference", None)
            if schema_inference is None:
                # this uses the internal datafusion engine that does inference with a limited read
                schema = getattr(ls.connect(), methodname)(source).schema()
            else:
                # user can pass a function that takes only source
                schema = schema_inference(source)

        table_name = table_name or util.gen_name(methodname)
        read_kwargs = tuple(
            {
                "source": source,
                "table_name": table_name,
                **kwargs,
            }.items()
        )
        op = Read(
            method=original_method,
            name=table_name,
            schema=schema,
            source=self,
            read_kwargs=read_kwargs,
        )
        return op.to_expr()

    reader_hotfix.__name__ = methodname
    reader_hotfix.__qualname__ = methodname
    return reader_hotfix


for methodname, target_tokenized in (
    ("read_csv", "259e465beb2caf194be6cae9385ea126"),
    ("read_parquet", "2eaafd515a77ffeb5e4faacd0653ce46"),
):
    cls = ibis.backends.pandas.Backend
    maybe_hotfix(
        cls,
        methodname,
        target_tokenized,
        make_reader_hotfix(cls, methodname),
    )
