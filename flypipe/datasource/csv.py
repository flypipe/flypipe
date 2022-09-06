from typing import List

from flypipe.converter.schema import SchemaConverter
from flypipe.datasource.datasource import DataSource
from flypipe.exceptions import ErrorModeNotSupported, ErrorDataFrameTypeNotSupported

from flypipe.mode import Mode


class CSV(DataSource):
    """Retrieves a csv file as dataframe"""

    @staticmethod
    def load(
        path: str,
        schema: List[SchemaConverter] = [],
        mode: Mode = Mode.PANDAS,
        spark=None,
    ):
        """
        Loads dataframe from csv file

        Parameters
        ----------
        path: str
            csv file locations
        schema: Lists[Schema], default None
            defines the schema of the loaded dataframe
        mode: Mode, default Mode.PANDAS
            defines the mode it will be loaded (pandas, pandas_on_spark or pyspark dataframe)
        spark : spark Session, default None

        Returns
        -------
        dataframe
            Dataframe in the type of mode

        Raises
        ------
        ErrorDataFrameTypeNotSupported
            if mode is spark, pandas_on_spark and self.spark is None

        ErrorModeNotSupported
            if mode is not pandas, spark, or pandas_on_spark
        """

        if mode == Mode.SPARK_SQL:
            raise ErrorModeNotSupported(mode, [Mode.PYSPARK, Mode.PANDAS_ON_SPARK])

        if mode in [Mode.PYSPARK, Mode.PANDAS_ON_SPARK] and spark is None:
            raise ErrorDataFrameTypeNotSupported()

        if mode == Mode.PANDAS:
            import pandas as pd

            return pd.read_csv(path)

        if mode in [Mode.PANDAS_ON_SPARK, Mode.PYSPARK]:
            from tests.utils.spark import spark

            df = spark.read.option("header", True).csv(path)
            if mode == Mode.PANDAS_ON_SPARK:
                df = df.to_pandas_on_spark()

            return df
