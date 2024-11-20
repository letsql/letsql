import operator

import letsql as ls
import scripts.zip_data as ZD
import scripts.settings as S
from letsql.common.caching import ParquetSnapshot
from letsql.expr.relations import (
    into_backend,
)


flight_to_rate_to_parquet = {
    "652200101100441": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101100441.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101100441.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101100441.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101100441.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101100441.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101100441.16p0.parquet",
    },
    "652200101120916": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101120916.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101120916.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101120916.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101120916.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101120916.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101120916.16p0.parquet",
    },
    "652200101121118": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121118.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121118.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121118.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121118.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121118.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121118.16p0.parquet",
    },
    "652200101121218": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121218.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121218.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121218.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121218.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121218.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121218.16p0.parquet",
    },
    "652200101121341": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121341.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121341.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121341.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121341.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121341.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121341.16p0.parquet",
    },
    "652200101121444": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121444.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121444.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121444.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121444.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121444.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121444.16p0.parquet",
    },
    "652200101121611": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121611.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121611.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121611.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121611.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121611.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101121611.16p0.parquet",
    },
    "652200101130002": {
        0.25: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101130002.0p25.parquet",
        1.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101130002.1p0.parquet",
        2.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101130002.2p0.parquet",
        4.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101130002.4p0.parquet",
        8.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101130002.8p0.parquet",
        16.0: "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com/Tail_652_1_parquet/652200101130002.16p0.parquet",
    },
}


def make_flight_to_rate_to_parquet(flight_datas):
    return {
        flight_data.flight: {
            rate: (
                "https://nasa-avionics-data-ml.s3.us-east-2.amazonaws.com"
                f"/{flight_data.parquet_dir.name}/{flight_data.flight}.{ZD.rate_to_rate_str(rate)}.parquet"
            )
            for rate in ZD.rate_to_columns
        }
        for flight_data in flight_datas
    }


def asof_join_flight_data_s3(flight, rate_to_parquet):
    """Create an expression for a particular flight's data"""
    con = ls.connect()
    # hack: register https object store
    con.read_csv(next(iter(rate_to_parquet.values())))
    # FIXME: do deferred read
    ts = tuple(
        con.read_parquet(parquet_path, ZD.rate_to_rate_str(rate)).mutate(
            flight=ls.literal(flight)
        )
        for rate, parquet_path in sorted(
            rate_to_parquet.items(), key=operator.itemgetter(0), reverse=True
        )
    )
    db_con = ls.duckdb.connect()
    (expr, *others) = (into_backend(t, db_con) for t in ts)
    for other in others:
        expr = expr.asof_join(other, on="time").drop(["time_right", "flight_right"])
    # remove ground data
    expr = expr[lambda t: t.GS != 0]
    return expr


def union_cached_asof_joined_flight_data(
    flight_to_rate_to_parquet, cls=ParquetSnapshot
):
    return ls.union(
        *(
            asof_join_flight_data_s3(flight, rate_to_parquet).cache(
                storage=cls(path=S.parquet_cache_path)
            )
            for (flight, rate_to_parquet) in flight_to_rate_to_parquet.items()
        )
    )


def record_normalized_expr(expr, name=None):
    import pathlib
    import pprint
    import dask

    name = name or dask.base.tokenize(expr)
    with pathlib.Path(name).with_suffix(".normalized.txt").open("wt") as fh:
        pprint.pprint(dask.base.normalize_token(expr), stream=fh)


return_type = "float64"
storage = ParquetSnapshot(path=S.parquet_cache_path)
(flight, rate_to_parquet) = next(iter(flight_to_rate_to_parquet.items()))
single_expr = asof_join_flight_data_s3(flight, rate_to_parquet)
record_normalized_expr(single_expr)
# expr = union_cached_asof_joined_flight_data(flight_to_rate_to_parquet)
# print(storage.cache.get_key(single_expr), storage.cache.get_key(expr))
