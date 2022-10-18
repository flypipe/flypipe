from functools import partial

from tabulate import tabulate
from flypipe.datasource.singleton import SingletonMeta
from flypipe.node import Node
from flypipe.node_type import NodeType

import pandas as pd

class SelectionNotFoundInTable(Exception):

    def __init__(self, table_name, source_columns, selected_columns):

        # Columns found or difference case sensitive
        all_cols = []
        for selected_col_ in selected_columns:
            found_selected_col = [c for c in source_columns if c == selected_col_]
            found_lower_selected_col = [c for c in source_columns if c.lower() == selected_col_.lower()]

            if found_selected_col:
                all_cols.append((found_selected_col[0], selected_col_, "found"))
            elif found_lower_selected_col:
                all_cols.append((found_lower_selected_col[0], selected_col_, f"Did you mean `{found_lower_selected_col[0]}`?"))

        error_df = pd.DataFrame(columns=[table_name, 'selection', 'error'])
        for c in all_cols:
            source_col, selected_col, error = c[0], c[1], c[2]
            error_df.loc[error_df.shape[0]] = [
                source_col,
                selected_col,
                error
            ]

        # Selected columns not found in table
        lower_source_columns = [c.lower() for c in source_columns]
        lower_selected_columns = [c.lower() for c in selected_columns]
        not_found_selected_columns = list(set(lower_selected_columns).difference(set(lower_source_columns)))
        for not_found_selected_column in not_found_selected_columns:

            selected_col = None
            for col in selected_columns:
                if col.lower() == not_found_selected_column:
                    selected_col = col
                    break

            error_df.loc[error_df.shape[0]] = [
                "",
                selected_col,
                "not found"
            ]

        error_df = (
            error_df
                .sort_values(['selection', table_name])
                .reset_index(drop=True)
        )

        msg_error =f"Flypipe: could not find some columns in `{table_name}`" \
                   f"\n\n{tabulate(error_df, headers='keys', tablefmt='mixed_outline')}\n"

        print(msg_error)

        super().__init__(msg_error)


class Spark(metaclass=SingletonMeta):
    """
    Abstract class to connect to Spark Datasource

    TODO: we need to make sure this is threadsafe, the current singleton implementation is not
    """

    def __init__(self, table):
        self.table = table
        self.all_selected_columns = []
        self.func = None

    def select(self, *columns):
        if isinstance(columns[0], list):
            columns = columns[0]
        else:
            columns = [column for column in columns]

        self.all_selected_columns = sorted(list(dict.fromkeys(self.all_selected_columns + columns)))
        func = partial(self.spark_datasource,
                       table=self.table,
                       columns=self.all_selected_columns)

        func.__name__ = self.table
        node = spark_datasource_node(type='pyspark',
                               description=f"Spark table {self.table}",
                               spark_context=True,
                               selected_columns = columns)

        func = node(func)
        func.grouped_selected_columns = self.all_selected_columns
        self.func = func
        return self.func

    @staticmethod
    def spark_datasource(spark, table, columns):
        assert spark is not None, 'Error: spark not provided'

        df = spark.table(table)

        df_cols = [col for col, _ in df.dtypes]

        if not set(columns).issubset(set(df_cols)):
            raise SelectionNotFoundInTable(table, df_cols, columns)

        if columns:
            df = df.select(columns)



        return df

class SparkDataSource(Node):
    node_type = NodeType.DATASOURCE


def spark_datasource_node(*args, **kwargs):
    """
    Decorator factory that returns the given function wrapped inside a Datasource Node class
    """

    def decorator(func):
        """TODO: I had to re-create graph in the decorator as selected_columns are set after the node has been created
        when creting a virtual datasource node, it is set the columns manually
        """

        kwargs_init = {k:v for k,v in kwargs.items() if k != 'selected_columns'}
        ds = SparkDataSource(func, *args, **kwargs_init)
        return ds.select(kwargs['selected_columns'])

    return decorator