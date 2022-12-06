import pytest


@pytest.fixture(scope="function")
def spark():
    from flypipe.tests.spark import spark

    return spark
