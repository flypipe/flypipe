from typing import List
from flypipe.datasource.datasource import DataSource
import pyspark.sql.functions as F

from flypipe.exceptions import ErrorTimeTravel


class Spark(DataSource):
    """
    Abstract class to connect to Spark Datasource

    Attributes
    ----------
    spark : spark Session
    """

    def __init__(self, spark):
        self.spark = spark

    def _filter_time_travel(self, df, time_travel_column, start_time_travel, end_time_travel):
        """
        Filter data from the queried dataframe

        Parameters
        ----------
        df: pyspark dataframe
            datframe to be filtered
        time_travel_column: str, default None
            column from the data that will be used to filter data using
            start_time_travel or end_time_travel
        start_time_travel: str, default None
            Filter data with time_travel_column >= start_time_travel,
        end_time_travel: str, default None
            Filter data with end_time_travel <= time_travel_column,

        Returns
        -------
        pyspark dataframe
            Filtered dataframe

        Raises
        ------
        ErrorTimeTravel
            if provided and time_travel_column is None, throws exception
            or if provided and time_travel_column is None, throws exception
        """

        if not time_travel_column and (start_time_travel or end_time_travel):
            raise ErrorTimeTravel('time_travel_column not specified')

        if start_time_travel:
            df = df.filter(F.col(time_travel_column) >= F.lit(start_time_travel))

        if end_time_travel:
            df = df.filter(F.col(time_travel_column) <= F.lit(end_time_travel))

        return df

    def load(self, table: str, columns: List[str] = None,
             time_travel_column: str = None,
             start_time_travel: object = None,
             end_time_travel: object = None):
        """
        Loads data from a spark data source.

        Parameters
        ----------
        table: str
            table name to be queried
        columns: list, default None
            list of columns to be selected from the data queried
        time_travel_column: str, default None
            column from the data that will be used to filter data using
            start_time_travel or end_time_travel
        start_time_travel: str, default None
            Filter data with time_travel_column >= start_time_travel,
            if provided and time_travel_column is None, throws exception
        end_time_travel: str, default None
            Filter data with end_time_travel <= time_travel_column,
            if provided and time_travel_column is None, throws exception

        Returns
        -------
        pyspark dataframe
            Dataframe from the spark data source

        Raises
        ------
        ErrorTimeTravel
            If no sound is set for the animal or passed in as a
            parameter.
        """

        df = self.spark.table(f"{table}")
        df = self._filter_time_travel(df, time_travel_column, start_time_travel, end_time_travel)

        if columns:
            return df.select(columns)

        return df