from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    import pandas as pd

from ._internal import SessionContext


class Backend:
    name = "letsql"
    dialect = "letsql"
    builder = None
    supports_in_memory_tables = True
    supports_arrays = True

    @property
    def version(self):
        return "0.1.1"

    def do_connect(
        self,
    ) -> None:
        self.con = SessionContext()

    def list_tables(
        self,
    ) -> list[str]:
        """List the available tables."""
        return self.con.tables()

    def table(self, name: str):
        """Get an ibis expression representing a DataFusion table.

        Parameters
        ----------
        name
            The name of the table to retrieve
        schema
            An optional schema for the table

        Returns
        -------
        Table
            A table expression
        """
        return self.con.table(name)

    def register(
        self,
        source: str | Path | pa.Table | pa.RecordBatch | pa.Dataset | pd.DataFrame,
        table_name: str | None = None,
    ):
        """Register a data set with `table_name` located at `source`.

        Parameters
        ----------
        source
            The data source(s). Maybe a path to a file or directory of
            parquet/csv files, a pandas dataframe, or a pyarrow table, dataset
            or record batch.
        table_name
            The name of the table
        """

        if isinstance(source, (str, Path)):
            first = str(source)

        if first.startswith(("parquet://", "parq://")) or first.endswith(
            ("parq", "parquet")
        ):
            self.con.register_parquet(table_name, first, file_extension=".parquet")
            return self.table(table_name)
