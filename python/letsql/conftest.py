from pathlib import Path

import pytest
from xdist.remote import Producer
from xdist.scheduler import LoadScopeScheduling

packages = (
    "python/letsql/backends/postgres/",
    "python/letsql/backends/let/",
    "python/letsql/common/utils",
    "python/letsql/tests/test_into_backend",
)


class LoadPackageScheduling(LoadScopeScheduling):
    def __init__(self, config: pytest.Config, log: Producer | None = None):
        super().__init__(config, log)

    def _split_scope(self, nodeid: str) -> str:
        parent_path = str(Path(nodeid).parent)
        if any(package in parent_path for package in packages):
            return "package"
        else:
            return nodeid


def pytest_xdist_make_scheduler(config, log):
    """xdist-pytest hook to set scheduler."""
    return LoadPackageScheduling(config, log)
