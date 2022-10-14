from typing import List, Union

from flypipe.mode import Mode


class DataframeTypeNotSupportedError(Exception):
    pass


class DataframeSchemasDoNotMatchError(Exception):
    pass


class DataframeDifferentDataError(DataframeSchemasDoNotMatchError):
    pass



class NodeTypeInvalidError(ValueError):
    pass


class DependencyNoSelectedColumnsError(ValueError):
    pass


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
