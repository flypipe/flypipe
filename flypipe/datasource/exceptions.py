from typing import List

from flypipe import Mode


class ErrorTimeTravel(Exception):
    pass


class SparkSessionNotProvided(Exception):
    pass


class DataFrameTypeNotSupported(Exception):
    pass


class DataframesNotEquals(Exception):
    pass


class DataframesSchemasDoNotMatch(DataframesNotEquals):
    pass


class DataframesDifferentData(DataframesNotEquals):
    pass


class ModeNotSupported(Exception):
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
