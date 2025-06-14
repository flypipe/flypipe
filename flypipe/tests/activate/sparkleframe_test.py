class TestSparkleFrame:
    def test_activate_deactivate(self):
        from sparkleframe.activate import activate, deactivate

        activate()
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        assert spark.__module__ == "sparkleframe.polarsdf.session"

        deactivate()

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        assert spark.__module__ == "pyspark.sql.session"

    def test_output_dataframe_is_sparkleframe_if_activated_environment(self):
        from sparkleframe.activate import activate

        activate()

        from flypipe import node

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        @node(
            type="pyspark",
            spark_context=True,
        )
        def my_node(spark):
            return spark.createDataFrame([1, 2, 3], ["a", "b", "c"])

        df = my_node.run(spark)
        assert df.__module__ == "sparkleframe.polarsdf.dataframe"
