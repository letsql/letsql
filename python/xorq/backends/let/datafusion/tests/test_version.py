import xorq as xq
from xorq.backends.let.datafusion import Backend


def test_version():
    assert xq.__version__ == Backend().version


def test_context_name():
    con = xq.connect()
    assert "let.SessionContext" in str(type(con.con))
