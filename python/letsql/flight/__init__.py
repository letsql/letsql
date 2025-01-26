import letsql as ls
from letsql.flight.backend import Backend
from letsql.flight.server import (
    BasicAuthServerMiddlewareFactory,
    FlightServerDelegate,
    NoOpAuthHandler,
)


DEFAULT_AUTH_MIDDLEWARE = {
    "basic": BasicAuthServerMiddlewareFactory(
        {
            "test": "password",
        }
    )
}


class BasicAuth:
    def __init__(self, username, password):
        self.username = username
        self.password = password


def to_basic_auth_middleware(basic_auth: BasicAuth) -> dict:
    assert basic_auth is not None

    return {
        "basic": BasicAuthServerMiddlewareFactory(
            {
                basic_auth.username: basic_auth.password,
            }
        )
    }


class FlightServer:
    def __init__(
        self,
        location=None,
        certificate_path=None,
        key_path=None,
        verify_client=False,
        root_certificates=None,
        auth: BasicAuth = None,
        connection=ls.duckdb.connect,
    ):
        self.location = location
        self.certificate_path = certificate_path
        self.key_path = key_path
        self.auth = auth

        tls_certificates = None
        basic_auth_middleware = None
        if key_path is not None and certificate_path is not None:
            with open(certificate_path, "rb") as cert_file:
                tls_cert_chain = cert_file.read()

            with open(key_path, "rb") as key_file:
                tls_private_key = key_file.read()

            tls_certificates = [(tls_cert_chain, tls_private_key)]
            basic_auth_middleware = to_basic_auth_middleware(auth)

        self.server = FlightServerDelegate(
            connection,
            location,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            auth_handler=NoOpAuthHandler(),
            middleware=basic_auth_middleware,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.server.__exit__(*args)


def make_con(
    con: FlightServer,
) -> Backend:
    from urllib.parse import urlparse

    url = urlparse(con.location)

    instance = Backend()
    instance.do_connect(
        host=url.hostname,
        port=url.port,
        username=con.auth.username,
        password=con.auth.password,
        tls_roots=con.certificate_path,
    )
    return instance


__all__ = ["FlightServer", "make_con", "BasicAuth"]
