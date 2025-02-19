import pytest

import xorq as xq
from xorq.examples import Example
from xorq.examples.core import whitelist


def test_whitelist():
    assert [name in dir(xq.examples) for name in whitelist]


def test_attributes():
    for name in whitelist:
        example = getattr(xq.examples, name)
        assert isinstance(example, Example)

    with pytest.raises(AttributeError):
        xq.examples.missing
