import os
import pytest


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestSparkleFramePySpark:
    """Tests for SparkleFrame activation - PySpark"""

    def test_activate_deactivate(self):
        from sparkleframe.activate import activate, deactivate

        activate()
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        assert spark.__module__ == "sparkleframe.polarsdf.session"

        deactivate()

        from flypipe.tests.spark import build_spark

        spark = build_spark()

        # Check the expected module based on RUN_MODE
        if os.environ.get("RUN_MODE") == "SPARK_CONNECT":
            assert spark.__module__ == "pyspark.sql.connect.session"
        else:
            assert spark.__module__ == "pyspark.sql.session"

    def test_output_dataframe_is_sparkleframe_if_activated_environment(self):
        from sparkleframe.activate import activate, deactivate

        activate()

        from flypipe import node

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        @node(
            type="pyspark",
            session_context=True,
        )
        def my_node(session):
            return session.createDataFrame([1, 2, 3], ["a", "b", "c"])

        df = my_node.run(spark)
        assert df.__module__ == "sparkleframe.polarsdf.dataframe"
        deactivate()
