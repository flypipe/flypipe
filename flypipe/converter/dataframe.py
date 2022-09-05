from flypipe import DataFrameType, dataframe_type


class DataFrame:

    @property
    def strategy(self, from_, to_, spark=None):
        pandas_to_spark = lambda df: df
        pandas_to_pandas_on_spark = lambda df: df

        pandas_on_spark_to_pandas = lambda df: df
        pandas_on_spark_to_spark = lambda df: df

        spark_to_pandas = lambda df: df
        spark_to_pandas_on_spark = lambda df: df

        if to_ in [DataFrameType.PYSPARK, DataFrameType.PANDAS_ON_SPARK]:
            pandas_to_spark = lambda df: spark.createDataFrame(df)
            pandas_to_pandas_on_spark = lambda df: spark.createDataFrame(df).to_pandas_on_spark()

            pandas_on_spark_to_pandas = lambda df: df.to_spark().toPandas()
            pandas_on_spark_to_spark = lambda df: df.to_spark()

            spark_to_pandas = lambda df: df.toPandas()
            spark_to_pandas_on_spark = lambda df: df.to_pandas_on_spark()


        return = {
                DataFrameType.PANDAS: {'to': DataFrameType.PYSPARK,
                                       'function': pandas_to_spark},
                DataFrameType.PANDAS: {'to': DataFrameType.PANDAS_ON_SPARK,
                                       'function': pandas_to_pandas_on_spark},

                DataFrameType.PANDAS_ON_SPARK: {'to': DataFrameType.PANDAS,
                                                'function': pandas_on_spark_to_pandas},
                DataFrameType.PANDAS_ON_SPARK: {'to': DataFrameType.PANDAS_ON_SPARK,
                                                'function': pandas_on_spark_to_spark}

                DataFrameType.PYSPARK: {'to': DataFrameType.PANDAS,
                                        'function': spark_to_pandas},
                DataFrameType.PYSPARK: {'to': DataFrameType.PANDAS_ON_SPARK,
                                        'function': spark_to_pandas_on_spark}
            }

    @property
    def schema(self, df):


    def convert(self, df, to_type: DataFrameType, spark=None):
        df_dtypes = schema
        from_type = dataframe_type(df)

        if from_type == to_type:
            return df

        df = self.strategy(from_type, to_type, spark=spark)

        if from_type == DataFrameType.PANDAS:
            df_dtypes = df.dtypes
            df = spark.createDataFrame(df)

            if to_type == DataFrameType.PANDAS_ON_SPARK:
                df = df.to_pandas_on_spark()

        elif to_type == DataFrameType.PANDAS:

            if from_type == DataFrameType.PANDAS_ON_SPARK:
                pass









a = 1
b = 2
match a:
    case 1:
        print(1)
    case 2:
        print(2)


        pass