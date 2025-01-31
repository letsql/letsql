"""Initialize LetSQL module."""

from __future__ import annotations

import importlib
from inspect import signature
from types import NoneType
from typing import get_type_hints

from letsql import examples
from letsql.config import options
from letsql.expr import api
from letsql.expr.api import *  # noqa: F403
from letsql.backends.let import Backend
from letsql.expr.letsql_expr import wrap_with_bridge_expr
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
        import ibis

        module = entry_point.load()
        backend = module.Backend()
        backend.register_options()

        def get_return_types(cls):
            # Get all methods in the class
            methods = [
                attr
                for attr in dir(cls)
                if callable(getattr(cls, attr)) and not attr.startswith("_")
            ]

            return_types = {}
            for method_name in methods:
                method = getattr(cls, method_name)

                # Option 1: Using get_type_hints (preferred for modern Python)
                try:
                    hints = get_type_hints(method)
                    return_type = hints.get("return")
                    return_types[method_name] = return_type
                except Exception:
                    # Option 2: Fallback to using signature
                    sig = signature(method)
                    return_type = sig.return_annotation
                    return_types[method_name] = (
                        return_type if return_type != sig.empty else None
                    )

            return return_types

        for method, return_type in get_return_types(type(backend)).items():
            if return_type is not None and not isinstance(return_type, NoneType):
                try:
                    if issubclass(return_type, ibis.Expr):
                        fun = getattr(backend, method)
                        setattr(backend, method, wrap_with_bridge_expr(fun))
                except Exception:
                    pass

        def connect(*args, **kwargs):
            return backend.connect(*args, **kwargs)

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


def connect(session_config: SessionConfig | None = None) -> Backend:
    """Create a LETSQL backend."""
    instance = Backend()
    instance.do_connect(session_config)
    return instance


def __getattr__(name):
    import ibis

    try:
        importlib.import_module(f"letsql.backends.{name}.hotfix")
    except ModuleNotFoundError:
        pass

    return load_backend(name) or ibis.__getattr__(name)


__version__ = importlib_metadata.version(__package__)
