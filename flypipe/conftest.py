import sys
import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logstart(nodeid, location):
    filename = location[0]
    test_name = nodeid.split("::")[-1]
    sys.stdout.flush()
    print(f"\n🏃 Running: {filename} -> {test_name}")


pytest.register_assert_rewrite("src.assert_pyspark_df_equal")

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True


@pytest.fixture(scope="function", autouse=False)
def spark(request):
    from flypipe.tests.spark import build_spark

    return build_spark()

@pytest.fixture(scope="function", autouse=False)
def snowflake_session(request):
    """
    Fixture for Snowflake Snowpark session.
    Uses local testing mode for tests.
    """
    from snowflake.snowpark import Session
    
    # Create local testing session
    session = Session.builder.config('local_testing', True).create()
    
    yield session
    
    # Cleanup
    session.close()
