from functools import partial

import letsql as ls
from letsql.examples.core import (
    get_name_to_suffix,
    get_table_from_csv,
    get_table_from_parquet,
    whitelist,
)


class Example:
    def __init__(self, name, load):
        self.load = load
        self.name = name

    def fetch(self, table_name=None, backend=None):
        if backend is None:
            backend = ls.connect()
        return self.load(backend, table_name or self.name)


def __dir__():
    return (
        "get_table_from_csv",
        "get_table_from_parquet",
        *whitelist,
    )


def __getattr__(name):
    lookup = get_name_to_suffix()

    if name not in lookup:
        raise AttributeError

    read = get_table_from_csv if lookup[name] == ".csv" else get_table_from_parquet
    return Example(name, partial(read, name))


__all__ = (
    "get_table_from_csv",
    "get_table_from_parquet",
    *whitelist,
)


def __getattr__(name):
    from letsql.vendor.ibis import examples as ibex

    return getattr(ibex, name)
