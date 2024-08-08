import pathlib
from typing import Union, Optional, Any

import ibis
from ibis.config import Config


class Cache(Config):
    """LETSQL cache configuration options

    Attributes
    ----------

    default_path : str

    """

    default_path: Union[str, pathlib.Path] = pathlib.Path(
        "~/.local/share/letsql"
    ).expanduser()
    key_prefix: str = "letsql_cache-"


class Interactive(Config):
    """Options controlling the interactive repr."""

    @property
    def max_rows(self) -> int:
        return ibis.options.repr.interactive.max_rows

    @max_rows.setter
    def max_rows(self, value: int):
        ibis.options.repr.interactive.max_rows = value

    @property
    def max_columns(self) -> Optional[int]:
        return ibis.options.repr.interactive.max_columns

    @max_columns.setter
    def max_columns(self, value: Optional[int]):
        ibis.options.repr.interactive.max_columns = value

    @property
    def max_length(self) -> int:
        return ibis.options.repr.interactive.max_length

    @max_length.setter
    def max_length(self, value: int):
        ibis.options.repr.interactive.max_length = value

    @property
    def max_string(self) -> int:
        return ibis.options.repr.interactive.max_string

    @max_string.setter
    def max_string(self, value: int):
        ibis.options.repr.interactive.max_string = value

    @property
    def max_depth(self) -> int:
        return ibis.options.repr.interactive.max_depth

    @max_depth.setter
    def max_depth(self, value: int):
        ibis.options.repr.interactive.max_depth = value

    @property
    def show_types(self) -> bool:
        return ibis.options.repr.interactive.show_types

    @show_types.setter
    def show_types(self, value: bool):
        ibis.options.repr.interactive.show_types = value


class Repr(Config):
    """Expression printing options.

    Attributes
    ----------
    interactive : Interactive
        Options controlling the interactive repr.
    """

    interactive: Interactive = Interactive()


class SQL(Config):
    """SQL-related options.

    Attributes
    ----------
    dialect : str
        Dialect to use for printing SQL when the backend cannot be determined.

    """

    dialect: str = "datafusion"


class Pins(Config):
    """SQL-related options.

    Attributes
    ----------
    dialect : str
        Dialect to use for printing SQL when the backend cannot be determined.

    """

    protocol: str = "gcs"
    path: str = "letsql-pins"
    storage_options: dict[str, Any] = dict(
        (
            ("cache_timeout", 0),
            ("token", "anon"),
        )
    )

    def get_board(self, **kwargs):
        import pins

        _kwargs = {
            **{
                "protocol": self.protocol,
                "path": self.path,
                "storage_options": self.storage_options,
            },
            **kwargs,
        }
        return pins.board(**_kwargs)

    def get_path(self, name, board=None):
        board = board or self.get_board()
        (path,) = board.pin_download(name)
        return path


class Options(Config):
    """LETSQL configuration options

    Attributes
    ----------
    cache : Cache
        Options controlling caching.
    backend : Optional[letsql.backends.let.Backend]
        The backend to use for execution.
    repr : Repr
        Options controlling expression printing.
    """

    cache: Cache = Cache()
    backend: Optional[Any] = None
    repr: Repr = Repr()
    sql: SQL = SQL()
    pins: Pins = Pins()

    @property
    def interactive(self) -> bool:
        """Show the first few rows of computing an expression when in a repl."""
        return ibis.options.interactive

    @interactive.setter
    def interactive(self, value: bool):
        ibis.options.interactive = value


options = Options()


def _backend_init():
    if (backend := options.backend) is not None:
        return backend

    import letsql

    options.backend = con = letsql.connect()
    return con
