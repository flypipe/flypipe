from typing import List

from flypipe.mode import Mode


class ErrorTimeTravel(Exception):
    pass


class ErrorErrorDataFrameTypeNotSupported(Exception):
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
        super().__init__(f"{mode} is not supported for CSV datasources. Please choose one of the supported modes "
                         f"{modes_supported}")
