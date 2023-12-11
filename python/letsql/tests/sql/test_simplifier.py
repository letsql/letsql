import ibis
from letsql.tests.util import assert_frame_equal


def test_where_predicate_logical_replacement(alltypes, df, snapshot):
    expr = alltypes.filter(
        [
            alltypes.float_col > 0,
            alltypes.smallint_col == 9,
            alltypes.int_col < alltypes.float_col * 2,
            ibis.literal(False),
        ]
    )
    snapshot.assert_match(expr.compile(), "out.sql")
    result = expr.execute()
    expected = df[
        (df["float_col"] > 0)
        & (df["smallint_col"] == 9)
        & (df["int_col"] < df["float_col"] * 2)
        & [False] * len(df)
    ]

    assert_frame_equal(result, expected)


def test_where_with_constant_evaluator(alltypes, df, snapshot):
    expr = alltypes.filter(
        [
            alltypes.float_col > 0,
            alltypes.smallint_col == 9,
            alltypes.int_col < alltypes.float_col * 2,
            ibis.literal(5) < ibis.literal(4),
        ]
    )
    snapshot.assert_match(expr.compile(), "out.sql")
    result = expr.execute()
    expected = df[
        (df["float_col"] > 0)
        & (df["smallint_col"] == 9)
        & (df["int_col"] < df["float_col"] * 2)
        & [False] * len(df)
    ]

    assert_frame_equal(result, expected)


def test_where_with_constant_evaluator_nested(alltypes, df, snapshot):
    expr = alltypes.filter(
        [
            alltypes.float_col > 0,
            alltypes.smallint_col == 9,
            alltypes.int_col < alltypes.float_col * 2,
            (ibis.literal(5) + ibis.literal(4)) < (ibis.literal(2) + ibis.literal(3)),
        ]
    )
    snapshot.assert_match(expr.compile(), "out.sql")
    result = expr.execute()
    expected = df[
        (df["float_col"] > 0)
        & (df["smallint_col"] == 9)
        & (df["int_col"] < df["float_col"] * 2)
        & [False] * len(df)
    ]

    assert_frame_equal(result, expected)
