import pandas as pd
from flypipe.node import node
from pyspark.sql import SparkSession
from flypipe.spark.context import register_spark_session


spark = SparkSession.builder.appName("flypipe").getOrCreate()
register_spark_session(spark)


@node(type="spark")
def fake_datasource1():
    return spark.createDataFrame(data=[['Albert', 30], ['Bernard', 20], ['Chris', 15]], schema=['name', 'age'])


@node(type="pandas")
def fake_datasource2():
    return pd.DataFrame(
        {
            "name": ["Albert", "Chris", "Chris", "Derek"],
            "fruit": ["Apple", "Banana", "Orange", "Banana"],
        }
    )


@node(type="pandas", inputs=[fake_datasource1, fake_datasource2])
def transformation(fake_datasource1, fake_datasource2):
    output = fake_datasource1.merge(fake_datasource2, on=["name"], how="inner")
    return output


print(transformation.run())
