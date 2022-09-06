from typing import List, Union

from flypipe.mode import Mode


class ErrorTimeTravel(Exception):
    pass


class ErrorDataFrameTypeNotSupported(Exception):
    pass


class DataFrameTypeNotSupported(Exception):
    pass


class ErrorErrorDataframesSchemasDoNotMatch(Exception):
    pass


class ErrorDataframesSchemasDoNotMatch(ErrorErrorDataframesSchemasDoNotMatch):
    pass


class ErrorDataframesDifferentData(ErrorErrorDataframesSchemasDoNotMatch):
    pass


class ErrorModeNotSupported(Exception):
    """Exception raised if mode is not supported

    Attributes:
        mode: Mode
            mode which caused the error
        modes_supported: List[Mode]
            list of mode supported
    """

    def __init__(self, mode: Mode, modes_supported: List[Mode]):
        super().__init__(
            f"{mode} is not supported for CSV datasources. Please choose one of the supported modes "
            f"{modes_supported}"
        )


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
