from enum import Enum


class DateFormat(Enum):
    """Which formatting style is being used for the formatting string in a Flypipe Date/DateTime type"""

    PYSPARK = "PYSPARK"
    PYTHON = "PYTHON"
    SNOWFLAKE = "SNOWFLAKE"

    def date_format(self):
        """Get the default date format for this format style"""
        DEFAULTS = {
            "PYSPARK": "yyyy-MM-dd",
            "PYTHON": "%Y-%m-%d",
            "SNOWFLAKE": "YYYY-MM-DD",
        }
        return DEFAULTS[self.value]

    def datetime_format(self):
        """Get the default datetime format for this format style"""
        DEFAULTS = {
            "PYSPARK": "yyyy-MM-dd H:m:s",
            "PYTHON": "%Y-%m-%d %H:%M:%S",
            "SNOWFLAKE": "YYYY-MM-DD HH24:MI:SS",
        }
        return DEFAULTS[self.value]
