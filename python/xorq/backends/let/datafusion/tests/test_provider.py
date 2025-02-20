import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
import xgboost as xgb
from sklearn.model_selection import train_test_split

import xorq as xo
from xorq.backends.let.datafusion.provider import IbisTableProvider
from xorq.vendor.ibis import udf


@pytest.fixture
def tmp_model_dir(tmpdir):
    # Create a temporary directory for the model
    model_dir = tmpdir.mkdir("models")
    return model_dir


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


@pytest.fixture(scope="session")
def data_dir():
    root = Path(__file__).absolute().parents[6]
    data_dir = root / "ci" / "ibis-testing-data"
    return data_dir


@pytest.fixture(scope="session")
def con(data_dir):
    conn = xo.connect()
    parquet_dir = data_dir / "parquet"
    conn.register(parquet_dir / "functional_alltypes.parquet", "functional_alltypes")

    return conn


def test_table_provider_scan(con):
    table_provider = IbisTableProvider(con.table("functional_alltypes"))
    batches = table_provider.scan()

    assert batches is not None
    assert isinstance(batches, pa.RecordBatchReader)


def test_table_provider_schema(con):
    table_provider = IbisTableProvider(con.table("functional_alltypes"))
    schema = table_provider.schema()
    assert schema is not None
    assert isinstance(schema, pa.Schema)


def test_register_model(data_dir, tmp_model_dir, con):
    data = pd.read_csv(data_dir / "csv" / "diamonds.csv")

    model = train_xgb(data, "reg:squarederror")
    model_path = os.path.join(tmp_model_dir, "model.json")
    model.save_model(model_path)
    con.register_xgb_model("diamonds_model", model_path)


def test_registered_model_udf(data_dir, tmp_model_dir, con):
    data = pd.read_csv(data_dir / "csv" / "diamonds.csv")
    model = train_xgb(data, "reg:squarederror")
    model_path = os.path.join(tmp_model_dir, "model.json")
    model.save_model(model_path)
    con.register_xgb_model("diamonds_model", model_path)

    features = ["carat", "depth", "x", "y", "z"]
    data_path = os.path.join(tmp_model_dir, "input.csv")
    data[features].to_csv(data_path, index=False)

    @udf.scalar.builtin
    def predict_xgb(
        model_name: str, carat: float, depth: float, x: float, y: float, z: float
    ) -> float:
        """predict builtin"""

    t = con.read_csv(table_name="diamonds_data", path=data_path).mutate(
        prediction=lambda t: predict_xgb(
            "diamonds_model", t.carat, t.depth, t.x, t.y, t.z
        )
    )

    result = xo.execute(t)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert "prediction" in result.columns
    assert result["prediction"].dtype == float
    assert len(result) == len(data)


def test_register_model_with_udf_output(data_dir, tmp_model_dir, con):
    data = pd.read_csv(data_dir / "csv" / "diamonds.csv")
    model = train_xgb(data, "reg:squarederror")
    model_path = os.path.join(tmp_model_dir, "model.json")
    model.save_model(model_path)
    predict_diamond = con.register_xgb_model("diamonds_model", model_path)

    features = ["carat", "depth", "x", "y", "z"]
    data_path = os.path.join(tmp_model_dir, "input.csv")
    data[features].to_csv(data_path, index=False)

    t = con.read_csv(table_name="diamonds_data", path=data_path).mutate(
        prediction=lambda t: predict_diamond(t.carat, t.depth, t.x, t.y, t.z)
    )

    result = xo.execute(t)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert "prediction" in result.columns
    assert result["prediction"].dtype == float
    assert len(result) == len(data)
