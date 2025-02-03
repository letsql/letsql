import letsql.vendor.ibis.common.exceptions as com
from letsql.common.utils.hotfix_utils import (
    hotfix,
    none_tokenized,
)
from letsql.vendor.ibis.backends.pandas import Backend as PandasBackend


@hotfix(
    PandasBackend,
    "drop_table",
    none_tokenized,
)
def drop_table(self, name: str, *, force: bool = False) -> None:
    if not force and name in self.dictionary:
        raise com.IbisError(
            "Cannot drop existing table. Call drop_table with force=True to drop existing table."
        )
    del self.dictionary[name]
    del self.schemas[name]
