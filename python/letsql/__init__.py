from .main import Backend

ba = Backend()


def con():
    ba.do_connect()
    return ba
