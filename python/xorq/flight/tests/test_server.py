import datetime

import pandas as pd
import pyarrow as pa
import pytest

import xorq as xq
from xorq.flight import FlightServer, FlightUrl, make_con
from xorq.flight.action import AddExchangeAction
from xorq.flight.exchanger import UDFExchanger


@pytest.mark.parametrize(
    "connection,port",
    [
        pytest.param(xq.duckdb.connect, 5005, id="duckdb"),
        pytest.param(xq.datafusion.connect, 5005, id="datafusion"),
        pytest.param(xq.connect, 5005, id="xorq"),
    ],
)
def test_port_in_use(connection, port):
    flight_url = FlightUrl.from_defaults(port=port)
    assert not flight_url.port_in_use(), f"Port {port} already in use"
    with pytest.raises(ValueError):
        with FlightServer(
            flight_url=flight_url,
            connection=connection,
        ) as _:
            with FlightServer(
                flight_url=flight_url,
                connection=connection,
            ) as _:
                pass


@pytest.mark.parametrize(
    "connection,port",
    [
        pytest.param(xq.duckdb.connect, 5005, id="duckdb"),
        pytest.param(xq.datafusion.connect, 5005, id="datafusion"),
        pytest.param(xq.connect, 5005, id="xorq"),
    ],
)
def test_register_and_list_tables(connection, port):
    flight_url = FlightUrl.from_defaults(port=port)
    assert not flight_url.port_in_use(), f"Port {port} already in use"

    with FlightServer(
        flight_url=flight_url,
        verify_client=False,
        connection=connection,
    ) as main:
        con = make_con(main)
        assert con.version is not None

        data = pa.table(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
        ).to_pandas()

        con.register(data, table_name="users")
        t = con.table("users")
        actual = xq.execute(t)

        assert t.schema() is not None
        assert main.flight_url.port_in_use()
        assert "users" in con.list_tables()
        assert isinstance(actual, pd.DataFrame)


@pytest.mark.parametrize(
    "connection,port",
    [
        pytest.param(xq.duckdb.connect, 5005, id="duckdb"),
        pytest.param(xq.datafusion.connect, 5005, id="datafusion"),
        pytest.param(xq.connect, 5005, id="xorq"),
    ],
)
def test_read_parquet(connection, port, parquet_dir):
    flight_url = FlightUrl.from_defaults(port=port)
    assert not flight_url.port_in_use(), f"Port {port} already in use"
    with FlightServer(
        flight_url=flight_url,
        verify_client=False,
        connection=connection,
    ) as main:
        con = make_con(main)
        batting = con.read_parquet(parquet_dir / "batting.parquet")
        assert xq.execute(batting) is not None


def instrument_reader(reader, prefix=""):
    def gen(reader):
        print(f"{prefix}first batch yielded at {datetime.datetime.now()}")
        yield next(reader)
        yield from reader
        print(f"{prefix}last batch yielded at {datetime.datetime.now()}")

    return pa.RecordBatchReader.from_batches(reader.schema, gen(reader))


@pytest.mark.parametrize(
    "connection,port",
    [
        pytest.param(xq.duckdb.connect, 5005, id="duckdb"),
        pytest.param(xq.datafusion.connect, 5005, id="datafusion"),
        pytest.param(xq.connect, 5005, id="xorq"),
    ],
)
def test_exchange(connection, port):
    flight_url = FlightUrl.from_defaults(port=port)
    assert not flight_url.port_in_use(), f"Port {port} already in use"

    def my_f(df):
        return df[["a", "b"]].sum(axis=1)

    with FlightServer(
        flight_url=flight_url,
        verify_client=False,
        connection=connection,
    ) as main:
        client = make_con(main).con
        udf_exchanger = UDFExchanger(
            my_f,
            schema_in=pa.schema(
                (
                    pa.field("a", pa.int64()),
                    pa.field("b", pa.int64()),
                )
            ),
            name="x",
            typ=pa.int64(),
            append=True,
        )
        client.do_action(AddExchangeAction.name, udf_exchanger, options=client._options)

        # a small example
        df_in = pd.DataFrame({"a": [1], "b": [2], "c": [100]})
        fut, rbr = client.do_exchange(
            udf_exchanger.command,
            pa.RecordBatchReader.from_stream(df_in),
        )
        df_out = rbr.read_pandas()
        writes_reads = fut.result()

        assert writes_reads["n_writes"] == writes_reads["n_reads"]
        assert df_out is not None

        # demonstrate streaming
        df_in = pd.DataFrame(
            {
                "a": range(100_000),
                "b": range(100_000, 200_000),
                "c": range(200_000, 300_000),
            }
        )
        fut, rbr = client.do_exchange_batches(
            udf_exchanger.command,
            instrument_reader(pa.Table.from_pandas(df_in).to_reader(max_chunksize=100)),
        )
        first_batch = next(rbr)
        first_batch_time = datetime.datetime.now()
        assert first_batch is not None, f"must get first batch by {first_batch_time}"

        rest = rbr.read_pandas()
        rest_time = datetime.datetime.now()
        assert rest is not None, f"must get first batch by {rest_time}"
        assert first_batch_time < rest_time

        writes_reads = fut.result()
        assert writes_reads["n_writes"] == 1000  # because
        assert writes_reads["n_reads"] == 1000
