from functools import partial

from tabulate import tabulate
from flypipe.datasource.singleton import SingletonMeta
from flypipe.node import Node, node
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


def Spark(table):
    @node(
        type='pyspark',
        description=f'Spark datasource on table {table}',
        tags=['datasource'],
        spark_context=True,
    )
    def spark_datasource(spark):
        return spark.table(table)
    spark_datasource.function.__name__ = table
    # __qualname__ gives the fullly qualified name of a function, the last section will be the __name__ of the function.
    # Unfortunately it doesn't seem to automatically adjust from the above change in __name__ so we do this manually.
    spark_datasource.function.__qualname__ = '.'.join(spark_datasource.function.__qualname__.split('.')[:-1] + [table])
    return spark_datasource
