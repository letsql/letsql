from typing import MutableMapping

import pandas as pd

import letsql.vendor.ibis.common.exceptions as com
from letsql.vendor.ibis.backends.pandas import Backend as IbisPandasBackend


class Backend(IbisPandasBackend):
    def do_connect(
        self,
        dictionary: MutableMapping[str, pd.DataFrame] | None = None,
    ) -> None:
        """Construct a client from a dictionary of pandas DataFrames.

        Parameters
        ----------
        dictionary
            An optional mapping of string table names to pandas DataFrames.

        Examples
        --------
        >>> import letsql as ls
        >>> ls.pandas.connect({"t": pd.DataFrame({"a": [1, 2, 3]})})  # doctest: +ELLIPSIS
        <ibis.backends.pandas.Backend object at 0x...>
        """
        self.dictionary = dictionary or {}
        self.schemas = {}

    def drop_table(self, name: str, *, force: bool = False) -> None:
        if not force and name in self.dictionary:
            raise com.LetSQLError(
                "Cannot drop existing table. Call drop_table with force=True to drop existing table."
            )
        del self.dictionary[name]
        del self.schemas[name]
