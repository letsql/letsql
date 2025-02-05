from functools import cache, partial

import letsql as ls


@cache
def _download_penguins():
    import pandas as pd

    url = "https://raw.githubusercontent.com/mesejo/palmerpenguins/master/palmerpenguins/data/penguins.csv"
    df = pd.read_csv(url)
    return df


def _cached_penguins(backend, table_name=None):
    return backend.register(_download_penguins(), table_name=table_name)


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


penguins = Example("penguins", _cached_penguins)
players = Example("players", partial(get_table_from_parquet, "awards_players"))
batting = Example("batting", partial(get_table_from_parquet, "batting"))
lending_club = Example("lending_club", partial(get_table_from_parquet, "lending-club"))
iris = Example("iris", partial(get_table_from_csv, "iris"))

__all__ = ["penguins", "players", "batting", "lending_club", "iris"]


def __getattr__(name):
    from letsql.vendor.ibis import examples as ibex

    return getattr(ibex, name)
