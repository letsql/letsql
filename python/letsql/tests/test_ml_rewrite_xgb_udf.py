from __future__ import annotations

import numpy as np

import letsql as ls
from letsql import _


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


def test_model_pruning(prediction_expr):
    original = prediction_expr.filter(_.carat < 1.0).execute()
    optimized = ls.expr.ml.rewrite_gbdt_expression(
        prediction_expr.filter(_.carat < 1.0)
    ).execute()

    np.testing.assert_array_almost_equal(
        original["pred"].values, optimized["pred"].values, decimal=3
    )


def test_pruned_model_exists_in_repr(prediction_expr):
    original = prediction_expr.filter(_.carat < 1.0)
    optimized = ls.expr.ml.rewrite_gbdt_expression(
        prediction_expr.filter(_.carat < 1.0)
    )

    assert "_pruned" not in repr(original)
    assert "_pruned" in repr(optimized)


def test_mixed_pruned_model_types(mixed_prediction_expr):
    original = mixed_prediction_expr.filter(_.carat < 1.0).execute()
    optimized = ls.expr.ml.rewrite_gbdt_expression(
        mixed_prediction_expr.filter(_.carat < 1.0)
    ).execute()

    np.testing.assert_array_almost_equal(
        original["pred"].values, optimized["pred"].values, decimal=3
    )


def test_prune_if_predicates_are_on_udf(mixed_model_path, mixed_feature_table):
    predict_udf = ls.expr.ml.make_xgboost_udf(mixed_model_path)
    t = mixed_feature_table.mutate(pred=predict_udf.on_expr).filter(_.pred <1)
    optimized = ls.expr.ml.rewrite_gbdt_expression(t)
    assert "pruned" not in repr(optimized)


def test_prune_if_predicates_are_not_features(mixed_model_path, mixed_feature_table):
    predict_udf = ls.expr.ml.make_xgboost_udf(mixed_model_path)
    t = mixed_feature_table.mutate(pred=predict_udf.on_expr).filter(_.target>=1)
    optimized = ls.expr.ml.rewrite_gbdt_expression(t)
    assert "pruned" not in repr(optimized)


def test_mixed_prune_greather_than(mixed_prediction_expr):
    original = mixed_prediction_expr.filter(_.carat > 1.0).execute()
    optimized = ls.expr.ml.rewrite_gbdt_expression(
        mixed_prediction_expr.filter(_.carat > 1.0)
    ).execute()

    np.testing.assert_array_almost_equal(
        original["pred"].values, optimized["pred"].values, decimal=3
    )


def test_mixed_prune_greather_than_or_equal(mixed_prediction_expr):
    original = mixed_prediction_expr.filter(_.carat >= 1.0).execute()
    optimized = ls.expr.ml.rewrite_gbdt_expression(
        mixed_prediction_expr.filter(_.carat >= 1.0)
    ).execute()

    np.testing.assert_array_almost_equal(
        original["pred"].values, optimized["pred"].values, decimal=3
    )
