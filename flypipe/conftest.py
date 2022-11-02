import pytest


@pytest.fixture(scope="function")
def spark():
    from tests.utils.spark import spark
    return spark