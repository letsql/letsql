"""Initialize LetSQL module."""

from __future__ import annotations

from xorq import examples
from xorq.config import options
from xorq.expr import api
from xorq.expr.api import *  # noqa: F403
from xorq.backends.let import Backend
from xorq.internal import SessionConfig

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
    eps = importlib_metadata.entry_points(group="xorq.backends")
    return sorted(eps)


def load_backend(name):
    if entry_point := next(
        (ep for ep in _load_entry_points() if ep.name == name), None
    ):
        import types

        import xorq as xq

        module = entry_point.load()
        backend = module.Backend()
        backend.register_options()

        def connect(*args, **kwargs):
            return backend.connect(*args, **kwargs)

        connect.__doc__ = backend.do_connect.__doc__
        connect.__wrapped__ = backend.do_connect
        connect.__module__ = f"letsql.{name}"

        proxy = types.ModuleType(f"letsql.{name}")
        setattr(xq, name, proxy)
        proxy.connect = connect
        proxy.compile = backend.compile
        proxy.has_operation = backend.has_operation
        proxy.name = name
        proxy._from_url = backend._from_url
        # Add any additional methods that should be exposed at the top level
        for attr in getattr(backend, "_top_level_methods", ()):
            setattr(proxy, attr, getattr(backend, attr))

        return proxy


def connect(session_config: SessionConfig | None = None) -> Backend:
    """Create a LETSQL backend."""
    instance = Backend()
    instance.do_connect(session_config)
    return instance


def __getattr__(name):
    from xorq.vendor import ibis

    return load_backend(name) or ibis.load_backend(name)


__version__ = importlib_metadata.version(__package__)
