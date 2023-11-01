from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def data_dir():
    root = Path(__file__).absolute().parents[3]
    data_dir = root / "ci" / "ibis-testing-data"
    return data_dir
