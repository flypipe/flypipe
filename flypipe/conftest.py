import sys
import pytest

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logstart(nodeid, location):
    filename = location[0]
    print(f"\n🏃 Running file: {filename}")

pytest.register_assert_rewrite("src.assert_pyspark_df_equal")

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True


@pytest.fixture(scope="function", autouse=False)
def spark(request):
    from flypipe.tests.spark import build_spark

    return build_spark()
