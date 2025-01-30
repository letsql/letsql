import base64
import logging
import secrets

import pyarrow as pa
import pyarrow.flight
from cloudpickle import loads

import letsql.flight.action as A
import letsql.flight.exchanger as E


logger = logging.getLogger(__name__)


class BasicAuthServerMiddlewareFactory(pa.flight.ServerMiddlewareFactory):
    """
    Middleware that implements username-password authentication.

    Parameters
    ----------
    creds: Dict[str, str]
        A dictionary of username-password values to accept.
    """

    def __init__(self, creds):
        self.creds = creds
        # Map generated bearer tokens to users
        self.tokens = {}

    def start_call(self, info, headers):
        """Validate credentials at the start of every call."""
        # Search for the authentication header (case-insensitive)
        auth_header = None
        for header in headers:
            if header.lower() == "authorization":
                auth_header = headers[header][0]
                break

        if not auth_header:
            raise pa.flight.FlightUnauthenticatedError("No credentials supplied")

        # The header has the structure "AuthType TokenValue", e.g.
        # "Basic <encoded username+password>" or "Bearer <random token>".
        auth_type, _, value = auth_header.partition(" ")

        if auth_type == "Basic":
            # Initial "login". The user provided a username/password
            # combination encoded in the same way as HTTP Basic Auth.
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(":")
            if not password or password != self.creds.get(username):
                raise pa.flight.FlightUnauthenticatedError(
                    "Unknown user or invalid password"
                )
            # Generate a secret, random bearer token for future calls.
            token = secrets.token_urlsafe(32)
            self.tokens[token] = username
            return BasicAuthServerMiddleware(token)
        elif auth_type == "Bearer":
            # An actual call. Validate the bearer token.
            username = self.tokens.get(value)
            if username is None:
                raise pa.flight.FlightUnauthenticatedError("Invalid token")
            return BasicAuthServerMiddleware(value)

        raise pa.flight.FlightUnauthenticatedError("No credentials supplied")


class BasicAuthServerMiddleware(pa.flight.ServerMiddleware):
    """Middleware that implements username-password authentication."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        """Return the authentication token to the client."""
        return {"authorization": f"Bearer {self.token}"}


class NoOpAuthHandler(pa.flight.ServerAuthHandler):
    """
    A handler that implements username-password authentication.

    This is required only so that the server will respond to the internal
    Handshake RPC call, which the client calls when authenticate_basic_token
    is called. Otherwise, it should be a no-op as the actual authentication is
    implemented in middleware.
    """

    def authenticate(self, outgoing, incoming):
        pass

    def is_valid(self, token):
        return ""


class FlightServerDelegate(pyarrow.flight.FlightServerBase):
    def __init__(
        self,
        con_callable,
        location=None,
        tls_certificates=None,
        verify_client=False,
        root_certificates=None,
        auth_handler=None,
        middleware=None,
    ):
        super(FlightServerDelegate, self).__init__(
            location=location,
            auth_handler=auth_handler,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            middleware=middleware,
        )
        self._conn = con_callable()
        self._location = location
        self.exchangers = E.exchangers
        self.actions = A.actions

    def _make_flight_info(self, query):
        """
        Create Flight info for a given SQL query

        Args:
            query: SQL query string
        """
        # Execute query to get schema and metadata
        kwargs = loads(query)
        expr = kwargs.pop("expr")
        result = self._conn.to_pyarrow_batches(expr, **kwargs).read_all()
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query)

        endpoints = [pyarrow.flight.FlightEndpoint(query, [self._location])]

        return pyarrow.flight.FlightInfo(
            result.schema, descriptor, endpoints, result.num_rows, result.nbytes
        )

    def get_flight_info(self, context, descriptor):
        """
        Get info about a specific query
        """
        query = descriptor.command
        return self._make_flight_info(query)

    def do_get(self, context, ticket):
        """
        Execute SQL query and return results
        """
        kwargs = loads(ticket.ticket)
        expr = kwargs.pop("expr")
        try:
            # Execute query and convert to Arrow table
            result = self._conn.to_pyarrow_batches(expr).read_all()
            return pyarrow.flight.RecordBatchStream(result)
        except Exception as e:
            raise pyarrow.flight.FlightServerError(f"Error executing query: {str(e)}")

    def do_put(self, context, descriptor, reader, writer):
        """
        Handle data upload - creates or updates a table
        """
        table_name = descriptor.command.decode("utf-8")
        data = reader.read_all()

        try:
            self._conn.register(data, table_name=table_name)
        except Exception as e:
            raise pyarrow.flight.FlightServerError(f"Error creating table: {str(e)}")

    def list_actions(self, context):
        """
        List available custom actions
        """
        return [(action.name, action.description) for action in self.actions.values()]

    def do_action(self, context, action):
        cls = self.actions.get(action.type)
        if cls:
            logger.info(f"doing action: {action.type}")
            yield from cls.do_action(self, context, action)
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def do_exchange(self, context, descriptor, reader, writer):
        if descriptor.descriptor_type != pyarrow.flight.DescriptorType.CMD:
            raise pa.ArrowInvalid("Must provide a command descriptor")
        command = descriptor.command.decode("ascii")
        if command in self.exchangers:
            logger.info(f"Doing exchange: {command}")
            return self.exchangers[command].exchange_f(context, reader, writer)
        else:
            raise pa.ArrowInvalid("Unknown command: {}".format(descriptor.command))
