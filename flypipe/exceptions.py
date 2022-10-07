from typing import List, Union

from flypipe.mode import Mode


class ErrorDataFrameTypeNotSupported(Exception):
    pass


class ErrorErrorDataframesSchemasDoNotMatch(Exception):
    pass


class ErrorDataframesSchemasDoNotMatch(ErrorErrorDataframesSchemasDoNotMatch):
    pass


class ErrorDataframesDifferentData(ErrorErrorDataframesSchemasDoNotMatch):
    pass

class ErrorNodeTypeInvalid(ValueError):
    pass

class ErrorDependencyNoSelectedColumns(ValueError):
    pass

class ErrorColumnNotInDataframe(Exception):
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
