"""Initialize LetSQL module."""

from __future__ import annotations

from letsql import examples
from letsql.config import options
from letsql.expr import api
from letsql.expr.api import *  # noqa: F403
from letsql.backends.let import Backend


try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata

__all__ = [  # noqa: PLE0604
    "api",
    "examples",
    "connect",
    "options",
    *api.__all__,
]


def connect() -> Backend:
    """Create a LETSQL backend."""
    instance = Backend()
    instance.do_connect()
    return instance


__version__ = importlib_metadata.version(__name__)
