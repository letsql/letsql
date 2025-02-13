"""Initialize LetSQL module."""

from __future__ import annotations

import importlib

from letsql import examples
from letsql.config import options
from letsql.expr import api
from letsql.expr.api import *  # noqa: F403
from letsql.backends.let import Backend
from letsql.internal import SessionConfig
import os 
from pathlib import Path
import yaml # noqa: F401

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
def resolve_env_vars(profile):
    """Resolve environment variables in the profile."""
    for key, value in profile.items():
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            resolved_value = os.getenv(env_var)
            if resolved_value is None:
                raise ValueError(f"Environment variable '{env_var}' not set")
            profile[key] = resolved_value
    return profile


def profiles_init(config_path: str = ".xorq/profiles.yaml"):
    """Initialize the profiles.yaml file with default profiles."""
    config_path = Path(config_path)
    if config_path.exists():
        raise FileExistsError(f"The file '{config_path}' already exists.")
    print(f"creating profile @ {config_path}")
    default_profiles = {
        "profiles": {
            "postgres": {
                "backend": "postgres",
                "host": "examples.letsql.com",
                "user": "${XORQ_POSTGRES_USER}",
                "password": "${XORQ_POSTGRES_PASSWORD}", # we could be opinionated about prefix 
                "database": "letsql"
            },
            "my_duckdb": {
                "backend": "duckdb",
                "database": ":memory:",
                "read_only": False,
                "extensions": None,
                "allocator_background_threads": False,
                "http_retry_backoff": 5
            },
            "default": {
                "backend": "letsql",
            },
          "snowflake": {
                "backend": "snowflake",
                "account": "${XORQ_SNOWFLAKE_ACCOUNT}",
                "database": "${XORQ_SNOWFLAKE_DATABASE}",
                "password": "${XORQ_SNOWFLAKE_PASSWORD}",
                "role": "${XORQ_SNOWFLAKE_ROLE}",
                "schema": "${XORQ_SNOWFLAKE_SCHEMA}",
                "user": "${XORQ_SNOWFLAKE_USER}",
                "warehouse": "${XORQ_SNOWFLAKE_WAREHOUSE}"
            }
        }
    }

    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w") as file:
        yaml.dump(default_profiles, file)

def profiles_connect(profile_name: str, config_path: str = ".xorq/profiles.yaml") -> Backend:
    """Create a LETSQL backend based on profile configuration."""
    with open(config_path, "r") as file:
        profiles = yaml.safe_load(file).get("profiles", {})

    if profile_name not in profiles:
        raise ValueError(f"Profile '{profile_name}' not found in {config_path}")

    profile = profiles[profile_name]
    profile = resolve_env_vars(profile)  # Resolve environment variables
    backend_name= profile.pop("backend")
    

    if backend_name == "letsql":
        backend_name ="let"

    backend = load_backend(backend_name)
    return backend.connect(**profile)


def __getattr__(name):
    import ibis

    try:
        importlib.import_module(f"letsql.backends.{name}.hotfix")
    except ModuleNotFoundError:
        pass

    return load_backend(name) or ibis.__getattr__(name)


__version__ = importlib_metadata.version(__package__)
