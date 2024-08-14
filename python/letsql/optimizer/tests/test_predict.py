import json
import os
from itertools import chain

import pandas as pd
import pytest
import xgboost as xgb
from pandas.api.types import is_float_dtype
from sklearn.model_selection import train_test_split

from letsql.internal import (
    SessionContext,
)


def train_xgb(
    data,
    objective="reg:squarederror",
    features=None,
    target="price",
    max_depth=8,
    n_estimators=100,
):
    # Split the data into features and target variable
    if features is None:
        features = ["carat", "depth", "x", "y", "z"]
    X = data[features]
    y = data[target]

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Instantiate an XGBoost regressor
    model = xgb.XGBRegressor(
        objective=objective,
        random_state=42,
        max_depth=max_depth,
        n_estimators=n_estimators,
    )

    # Train the model
    model.fit(X_train, y_train)

    # Return the trained model
    return model


def model_features(path):
    with open(path) as infile:
        json_data = json.load(infile)
        json_trees = json_data["learner"]["gradient_booster"]["model"]["trees"]

        indices = sorted(
            set(
                chain.from_iterable(
                    json_tree["split_indices"] for json_tree in json_trees
                )
            )
        )
        return {v: i for i, v in enumerate(indices)}


def test_predict_with_only_required_features(tmp_model_dir, data_dir, capsys):
    data = pd.read_csv(data_dir / "csv" / "diamonds.csv")

    model = train_xgb(data, "reg:squarederror", max_depth=2, n_estimators=3)
    model_path = os.path.join(tmp_model_dir, "model.json")
    model.save_model(model_path)

    features = ["carat", "depth", "x", "y", "z"]
    data_path = os.path.join(tmp_model_dir, "input.csv")
    data[features].to_csv(data_path, index=False)

    context = SessionContext()
    context.register_csv("diamonds", data_path)
    context.register_xgb_json_model("diamonds_model", model_path)

    query = "select predict_xgb('diamonds_model', *) from diamonds;"
    context.sql(query).explain()
    predictions = context.sql(query).to_pandas()

    captured = capsys.readouterr()
    expanded = 'predict_xgb(Utf8("diamonds_model"), carat, y)'
    assert expanded in captured.out

    assert len(model_features(model_path)) < len(features)
    assert len(predictions) == len(data)
    assert is_float_dtype(predictions.squeeze())


def test_predict_with_filter(tmp_model_dir, data_dir, capsys):
    data = pd.read_csv(data_dir / "csv" / "diamonds.csv")

    model = train_xgb(data, "reg:squarederror")
    model_path = os.path.join(tmp_model_dir, "model.json")
    model.save_model(model_path)

    features = ["carat", "depth", "x", "y", "z"]
    data_path = os.path.join(tmp_model_dir, "input.csv")
    data[features].to_csv(data_path, index=False)

    context = SessionContext()
    context.register_csv("diamonds", data_path)
    context.register_xgb_json_model("diamonds_model", model_path)

    query = "select predict_xgb('diamonds_model', *) from diamonds where x < 4.5;"
    context.sql(query).explain()
    predictions = context.sql(query).to_pandas()

    captured = capsys.readouterr()
    expanded = 'predict_xgb(Utf8("diamonds_model"), carat, depth, x, y, z)'
    assert expanded in captured.out

    assert len(predictions) == sum(data.x < 4.5)
    assert is_float_dtype(predictions.squeeze())


def test_predict_fails_when_model_does_not_exist(tmp_model_dir, data_dir):
    data = pd.read_csv(data_dir / "csv" / "diamonds.csv")

    features = ["carat", "depth", "x", "y", "z"]
    data_path = os.path.join(tmp_model_dir, "input.csv")
    data[features].to_csv(data_path, index=False)

    context = SessionContext()
    context.register_csv("diamonds", data_path)

    query = "select predict_xgb('missing_model', *) from diamonds"

    with pytest.raises(BaseException):
        context.sql(query).to_pandas()
