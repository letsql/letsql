"""Initialize LetSQL module."""

from __future__ import annotations

import importlib

import ibis

from letsql import examples
from letsql.backends.let import Backend
from letsql.config import options
from letsql.expr import api
from letsql.expr.api import *  # noqa: F403
from letsql.expr.letsql_expr import wrap_with_bridge
from letsql.internal import SessionConfig

try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata

__all__ = [  # noqa: PLE0604
    "api",
    "examples",
    "connect",
    "options",
    "SessionConfig",
    *api.__all__,
]

_CUSTOM_BACKENDS = ["postgres", "snowflake"]


def _load_entry_points():
    eps = importlib_metadata.entry_points(group="letsql.backends")
    return sorted(eps)


def load_backend(name):
    if entry_point := next(
        (ep for ep in _load_entry_points() if ep.name == name), None
    ):
        import types

        import letsql as ls

        module = entry_point.load()
        backend = module.Backend()
        backend.register_options()

        def connect(*args, **kwargs):
            instance = backend.connect(*args, **kwargs)

            for method in methods_to_wrap:
                if (fun := getattr(instance, method, None)) and callable(fun):
                    setattr(instance, method, wrap_with_bridge(fun))

            return instance

        connect.__doc__ = backend.do_connect.__doc__
        connect.__wrapped__ = backend.do_connect
        connect.__module__ = f"letsql.{name}"

        proxy = types.ModuleType(f"letsql.{name}")
        setattr(ls, name, proxy)
        proxy.connect = connect
        proxy.compile = backend.compile
        proxy.has_operation = backend.has_operation
        proxy.name = name
        proxy._from_url = backend._from_url
        # Add any additional methods that should be exposed at the top level
        for attr in getattr(backend, "_top_level_methods", ()):
            setattr(proxy, attr, getattr(backend, attr))

        return proxy


methods_to_wrap = [
    "create_table",
    "create_view",
    "read_csv",
    "read_delta",
    "read_geo",
    "read_json",
    "read_mysql",
    "read_parquet",
    "read_postgres",
    "read_record_batches",
    "read_sqlite",
    "register",
    "table",
]


def connect(session_config: SessionConfig | None = None) -> Backend:
    """Create a LETSQL backend."""
    instance = Backend()
    instance.do_connect(session_config)

    for method in methods_to_wrap:
        if (fun := getattr(instance, method, None)) and callable(fun):
            setattr(instance, method, wrap_with_bridge(fun))
    return instance


def __getattr__(name):
    try:
        importlib.import_module(f"letsql.backends.{name}.hotfix")
    except ModuleNotFoundError:
        pass

    return load_backend(name) or ibis.__getattr__(name)


__version__ = importlib_metadata.version(__package__)
