from typing import Union

import pandas as pd
from tabulate import tabulate


class DataframeTypeNotSupportedError(Exception):
    """Error raised if the dataframe type is not supported"""


class DataframeSchemasDoNotMatchError(Exception):
    """Error raised if a dataframe schema is not compatible with what was expected"""


class DataframeDifferentDataError(DataframeSchemasDoNotMatchError):
    """Error raised if the dataframe data is different"""


class DependencyNoSelectedColumnsError(ValueError):
    """Error raised if no columns are selected from the dataframe"""


class ColumnNotInDataframeError(Exception):
    """Exception raised if column is not in dataframe

    Attributes:
        non_existing_columns: str or list
            columns that do not exists in dataframe
    """

    def __init__(self, non_existing_columns: Union[str, list]):
        non_existing_columns = (
            [non_existing_columns]
            if isinstance(non_existing_columns, str)
            else non_existing_columns
        )
        super().__init__(
            f"The following columns {non_existing_columns} do not exist in the dataframe"
        )


class DataFrameMissingColumns(Exception):
    """
    Exception raised when the dataframe is missing some expected columns. Primarily responsible for generating a nice
    table to give the reader a high level picture of which columns are available, which aren't and which columns have
    been requested.
    """

    def __init__(
        self, source_columns, selected_columns
    ):  # pylint: disable=too-many-locals

        # Columns found or difference case sensitive
        all_cols = []
        for selected_column in selected_columns:
            found_selected_col = [c for c in source_columns if c == selected_column]
            found_lower_selected_col = [
                c for c in source_columns if c.lower() == selected_column.lower()
            ]

            if found_selected_col:
                all_cols.append((found_selected_col[0], selected_column, "found"))
            elif found_lower_selected_col:
                all_cols.append(
                    (
                        found_lower_selected_col[0],
                        selected_column,
                        f"Did you mean `{found_lower_selected_col[0]}`?",
                    )
                )

        error_df = pd.DataFrame(columns=["dataframe", "selection", "error"])
        for c in all_cols:
            source_col, selected_col, error = c[0], c[1], c[2]
            error_df.loc[error_df.shape[0]] = [source_col, selected_col, error]

        # Selected columns not found in table
        lower_source_columns = [c.lower() for c in source_columns]
        lower_selected_columns = [c.lower() for c in selected_columns]
        not_found_selected_columns = list(
            set(lower_selected_columns).difference(set(lower_source_columns))
        )
        for not_found_selected_column in not_found_selected_columns:

            selected_col = None
            for col in selected_columns:
                if col.lower() == not_found_selected_column:
                    selected_col = col
                    break

            error_df.loc[error_df.shape[0]] = ["", selected_col, "not found"]

        error_df = error_df.sort_values(["selection", "dataframe"]).reset_index(
            drop=True
        )

        msg_error = (
            f"Flypipe: could not find some columns in the dataframe"
            f"\n\nOutput Dataframe columns: {source_columns}"
            f"\nGraph selected columns: {selected_columns}"
            f"\n\n\n{tabulate(error_df, headers='keys', tablefmt='mixed_outline')}\n"
        )

        print(msg_error)

        super().__init__(msg_error)
