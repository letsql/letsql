from __future__ import annotations

import numpy as np
from letsql import _
import letsql as ls


def test_predicate_collection(prediction_expr):
    expr = prediction_expr.filter(_.carat < 1.0)
    predicates = ls.expr.ml.collect_predicates(expr.op())

    assert len(predicates) == 1
    assert predicates[0]["column"] == "carat"
    assert predicates[0]["op"] == "Less"
    assert predicates[0]["value"] == 1.0


def test_multiple_predicates(prediction_expr):
    expr = prediction_expr.filter([_.carat < 1.0, _.depth > 60, _.table >= 55])

    predicates = ls.expr.ml.collect_predicates(expr.op())

    assert len(predicates) == 3
    assert {p["op"] for p in predicates} == {"Less", "Greater", "GreaterEqual"}


def test_model_pruning(feature_table, float_model_path):
    predict_fn = ls.expr.ml.make_xgboost_udf(float_model_path)
    prediction_expr = feature_table.mutate(pred=predict_fn.on_expr)
    original = prediction_expr.filter(_.carat < 1.0).execute()
    optimized = ls.expr.ml.rewrite_gbdt_expression(
        prediction_expr.filter(_.carat < 1.0)
    ).execute()

    np.testing.assert_array_almost_equal(
        original["pred"].values, optimized["pred"].values, decimal=3
    )
