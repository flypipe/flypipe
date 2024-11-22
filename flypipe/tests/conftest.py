import pytest


@pytest.fixture(scope="function")
def spark():
    # Put the import locally otherwise it will shadow this identically named function
    from flypipe.tests.spark import spark

    return spark
