from typing import Union
import pyspark.sql.functions as F
from pyspark.pandas.typedef import pandas_on_spark_type, spark_type_to_pandas_dtype
from pyspark.sql.types import BooleanType

from flypipe.exceptions import ErrorColumnNotInDataframe
from flypipe.utils import dataframe_type, DataFrameType, get_schema


class Type:
    """Casts dataframe columns.
    When converting Spark dataframe to Pandas (on spark), some datatypes is converted as object,
    for example Spark Dataframe with a column DateType is converted to object.
    This class converts the the dataframe to the approppriate data type accordingly to
    pandas_on_spark_type and spark_type_to_pandas_dtype
    """

    def __init__(self):
        self.pandas_type, self.spark_type = pandas_on_spark_type(
            spark_type_to_pandas_dtype(self.spark_data_type())
        )

    def __repr__(self):
        return f"pandas_type {self.pandas_type}, spark_type: {self.spark_type}"

    @property
    def spark_data_type(self):
        raise NotImplementedError

    def columns(self, df, column: Union[str, list]):
        """Receives a str and return a list[str] or
        if receives a list, returns the list

        Parameters
        ----------
        df : dataframe
            dataframe in wich the column(s) to be casted

        column : str or list
            column(s) to casted

        Returns
        -------
        list
            list of columns

        Raises
        ------
        ErrorColumnNotInDataframe
            if column(s) provided to be cast do not exist in the dataframe
        """
        columns_to_cast = column if isinstance(column, list) else [column]
        non_existing_columns = list(set(columns_to_cast) - set(df.columns))
        if non_existing_columns:
            raise ErrorColumnNotInDataframe(non_existing_columns)

        return columns_to_cast

    def cast(self, df, column: Union[str, list]):
        """Receives a str and return a list[str] or
        if receives a list, returns the list

        Parameters
        ----------
        df : dataframe
            dataframe to have column(s) casted
        column: str or list
            column(s) to be casted

        Returns
        -------
        dataframe
            dataframe with given column(s) casted to the DataType defined by the child in spark_data_type
        """
        columns_to_cast = self.columns(df, column)

        schema = get_schema(df, columns_to_cast)
        if dataframe_type(df) == DataFrameType.PYSPARK:

            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.spark_type and column in columns_to_cast
            }

            for col in columns_to_cast.keys():
                df = df.withColumn(col, F.col(col).cast(self.spark_type))

        else:
            columns_to_cast = {
                column: data_type
                for column, data_type in schema.items()
                if data_type != self.pandas_type and column in columns_to_cast
            }

            if columns_to_cast:
                columns_to_cast = list(columns_to_cast.keys())
                df[columns_to_cast] = df[columns_to_cast].astype(self.pandas_type)

        return df


class Boolean(Type):
    """Casts dataframe to boolean"""

    spark_data_type = BooleanType
