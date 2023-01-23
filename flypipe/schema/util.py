from enum import Enum


class DateFormat(Enum):
    """Which formatting style is being used for the formatting string in a Flypipe Date/DateTime type"""

    PYSPARK = 1
    PYTHON = 2
