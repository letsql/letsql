import functools
import pathlib

import letsql as ls


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


def get_table_from_parquet(name, backend, table_name):
    parquet_file = ls.config.options.pins.get_path(name)
    return backend.read_parquet(parquet_file, table_name=table_name)


def get_table_from_csv(name, backend, table_name):
    parquet_file = ls.config.options.pins.get_path(name)
    return backend.read_csv(parquet_file, table_name=table_name)
