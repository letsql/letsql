from abc import (
    ABC,
    abstractmethod,
)

import pyarrow as pa
import pyarrow.flight as paf
import toolz
from cloudpickle import dumps, loads


make_flight_result = toolz.compose(
    paf.Result,
    pa.py_buffer,
    dumps,
)


class AbstractAction(ABC):
    @classmethod
    @property
    @abstractmethod
    def name(cls):
        pass

    @classmethod
    @property
    @abstractmethod
    def description(cls):
        pass

    @classmethod
    @abstractmethod
    def do_action(cls, server, context, action):
        pass


class HealthCheckAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "healthcheck"

    @classmethod
    @property
    def description(cls):
        return "NOP: check that communication is established"

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result(None)


class ListExchangesAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "list-exchanges"

    @classmethod
    @property
    def description(cls):
        return "Get a list of all exchange commands available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result(
            tuple(exchanger.command for exchanger in server.exchangers.values())
        )


class AddActionAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "add-action"

    @classmethod
    @property
    def description(cls):
        return "Add an action to the server's repertoire of actions"

    @classmethod
    def do_action(cls, server, context, action):
        action_class = loads(action.body)
        server.actions[action_class.name] = action_class
        yield make_flight_result(None)


class AddExchangeAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "add-exchange"

    @classmethod
    @property
    def description(cls):
        return "Add an exchange to the server's repertoire of exchanges"

    @classmethod
    def do_action(cls, server, context, action):
        exchange_class = loads(action.body)
        server.exchangers[exchange_class.command] = exchange_class
        yield make_flight_result(None)


class QueryExchangeAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "query-exchange"

    @classmethod
    @property
    def description(cls):
        return "Get metadata about a particular exchange available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        exchange_name = loads(action.body)
        exchanger = server.exchangers.get(exchange_name)
        query_result = exchanger.query_result if exchanger else None
        yield make_flight_result(query_result)


class ListTablesAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "list_tables"

    @classmethod
    @property
    def description(cls):
        return "Get the names of all tables available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        kwargs = loads(action.body)
        tables = server._conn.list_tables(**kwargs)
        yield make_flight_result(tables)


class TableInfoAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "table_info"

    @classmethod
    @property
    def description(cls):
        return "Get info about a particular table available on this server."

    @classmethod
    def do_action(cls, server, context, action):
        kwargs = loads(action.body)
        table_name = kwargs.pop("table_name")
        schema = server._conn.get_schema(table_name, **kwargs)
        yield make_flight_result(schema)


class DropTableAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "drop_table"

    @classmethod
    @property
    def description(cls):
        return "Drop a table on this server."

    @classmethod
    def do_action(cls, server, context, action):
        table_name = loads(action.body)
        server._conn.execute(table_name)
        yield make_flight_result(f"dropped table {table_name}")


class DropViewAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "drop_view"

    @classmethod
    @property
    def description(cls):
        return "Drop a view on this server."

    @classmethod
    def do_action(cls, server, context, action):
        table_name = loads(action.body)
        server._conn.drop_view(table_name)
        yield make_flight_result(f"dropped view {table_name}")


class ReadParquetAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "read_parquet"

    @classmethod
    @property
    def description(cls):
        return "Read parquet files into this server."

    @classmethod
    def do_action(cls, server, context, action):
        args = loads(action.body)

        table_name = args["table_name"]
        source_list = args["source_list"]

        table = server._conn.read_parquet(source_list, table_name)
        yield make_flight_result(table.get_name())


class VersionAction(AbstractAction):
    @classmethod
    @property
    def name(cls):
        return "version"

    @classmethod
    @property
    def description(cls):
        return "Return the version of the underlying backend"

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result(server._conn.version)


class GetSchemaQueryAction(AbstractAction):
    @classmethod
    def name(cls):
        return "get_schema_using_query"

    @classmethod
    def description(cls):
        return "Get the schema of query result"

    @classmethod
    def do_action(cls, server, context, action):
        query = loads(action.body)
        schema = server._conn._get_schema_using_query(query)
        yield make_flight_result(schema)


actions = {
    action.name: action
    for action in (
        HealthCheckAction,
        ListExchangesAction,
        QueryExchangeAction,
        AddActionAction,
        AddExchangeAction,
        ListTablesAction,
        TableInfoAction,
        DropTableAction,
        DropViewAction,
        ReadParquetAction,
        VersionAction,
        GetSchemaQueryAction,
    )
}
