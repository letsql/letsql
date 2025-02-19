import functools
import pathlib

import letsql as ls
from letsql.common.utils.defer_utils import (
    deferred_read_csv,
    deferred_read_parquet,
)


whitelist = [
    "astronauts",
    "awards_players",
    "batting",
    "diamonds",
    "functional_alltypes",
    "iris",
    "penguins",
]


@functools.cache
def get_name_to_suffix():
    board = ls.options.pins.get_board()
    dct = {
        name: pathlib.Path(board.pin_meta(name).file).suffix
        for name in board.pin_list()
        if name in whitelist
    }
    return dct


def get_table_from_name(name, backend, table_name=None, deferred=True, **kwargs):
    suffix = get_name_to_suffix().get(name)
    match suffix:
        case ".parquet":
            if deferred:
                method = functools.partial(deferred_read_parquet, backend)
            else:
                method = backend.read_parquet
        case ".csv":
            if deferred:
                method = functools.partial(deferred_read_csv, backend)
            else:
                method = backend.read_csv
        case _:
            raise ValueError
    path = ls.config.options.pins.get_path(name)
    return method(path, table_name=table_name or name, **kwargs)
