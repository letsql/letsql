from ibis.common.grounds import Singleton
from ibis.expr.datatypes import Variadic


class LargeString(Variadic, Singleton):
    """A type representing a large_string.

    Notes
    -----
    Because of differences in the way different backends handle strings, we
    cannot assume that strings are UTF-8 encoded.

    """

    scalar = "LargeStringScalar"
    column = "LargeStringColumn"
