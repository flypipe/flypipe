import sys

import pytest

from flypipe.tests.spark import spark as spark_session

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True


@pytest.fixture(scope="class", autouse=False)
def spark():
    return spark_session
