import pathlib
from functools import cache, partial

import letsql as ls


def get_table_from_parquet(name, backend, table_name):
    parquet_file = ls.config.options.pins.get_path(name)
    return backend.read_parquet(parquet_file, table_name=table_name)


def get_table_from_csv(name, backend, table_name):
    parquet_file = ls.config.options.pins.get_path(name)
    return backend.read_csv(parquet_file, table_name=table_name)


class Example:
    def __init__(self, name, load):
        self.load = load
        self.name = name

    def fetch(self, table_name=None, backend=None):
        if backend is None:
            backend = ls.connect()
        return self.load(backend, table_name or self.name)


@cache
def get_name_to_suffix():
    board = ls.options.pins.get_board()
    dct = {
        name: pathlib.Path(board.pin_meta(name).file).suffix
        for name in board.pin_list()
        if pathlib.Path(board.pin_meta(name).file).suffix not in (".json",)
    }
    return dct


def __dir__():
    names = get_name_to_suffix().keys()
    return list(names)


def __getattr__(name):
    lookup = get_name_to_suffix()

    if name not in lookup:
        raise AttributeError

    read = get_table_from_csv if lookup[name] == ".csv" else get_table_from_parquet
    return Example(name, partial(read, name))


def __getattr__(name):
    from letsql.vendor.ibis import examples as ibex

    return getattr(ibex, name)
