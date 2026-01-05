from flypipe.schema.util import DateFormat


class Type:
    """Base class for Flypipe types"""

    @classmethod
    def key(cls):
        return cls.__name__.lower()

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def valid_values(self):
        return None

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.name}()"


class Unknown(Type):
    """Special type that is used when the type of column is unknown"""


class Boolean(Type):
    """Flypipe boolean type"""

    VALID_VALUES = {True, False, 1, 0}

    @property
    def valid_values(self):
        return self.VALID_VALUES


class Byte(Type):
    """Flypipe byte type"""


class Binary(Type):
    """Flypipe binary type"""


class Integer(Type):
    """Flypipe integer type"""


class Short(Type):
    """Flypipe short type"""


class Long(Type):
    """Flypipe long type"""


class Float(Type):
    """Flypipe float type"""


class Double(Type):
    """Flypipe double type"""


class String(Type):
    """Flypipe string type"""


class Decimal(Type):
    """Flypipe decimal type"""

    def __init__(self, precision: int = 13, scale: int = 2):
        self.precision = precision
        self.scale = scale

    def __repr__(self):
        return f"{self.name}({self.precision}, {self.scale})"


class Date(Type):
    """Flypipe date type"""

    PYTHON_PYSPARK_DATETIME_SYMBOL_MAP = {
        "%A": "EEEE",
        "%B": "MMMM",
        "%H": "H",
        "%I": "h",
        "%M": "m",
        "%S": "s",
        "%Y": "yyyy",
        "%a": "E",
        "%b": "MMM",
        "%d": "dd",
        "%f": "SSSSSS",
        "%j": "yyy",
        "%m": "MM",
        "%p": "a",
        "%y": "yy",
        "%z": "z",
    }
    PYSPARK_PYTHON_DATETIME_SYMBOL_MAP = {
        pyspark_symbol: python_symbol
        for python_symbol, pyspark_symbol in PYTHON_PYSPARK_DATETIME_SYMBOL_MAP.items()
    }

    PYTHON_SNOWFLAKE_DATETIME_SYMBOL_MAP = {
        "%A": "DY",  # Abbreviated day of week (Snowflake doesn't have full day name)
        "%B": "MMMM",
        "%H": "HH24",
        "%I": "HH12",
        "%M": "MI",
        "%S": "SS",
        "%Y": "YYYY",
        "%a": "DY",
        "%b": "MON",
        "%d": "DD",
        "%f": "FF",
        "%m": "MM",
        "%p": "AM",
        "%y": "YY",
        "%z": "TZHTZM",  # Timezone offset (e.g., +0000, -0700)
        # Note: %j (day of year) has no direct Snowflake equivalent
    }
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP = {
        snowflake_symbol: python_symbol
        for python_symbol, snowflake_symbol in PYTHON_SNOWFLAKE_DATETIME_SYMBOL_MAP.items()
    }
    # Additional Snowflake-specific mappings
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["PM"] = "%p"
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["FF9"] = "%f"
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["FF6"] = "%f"
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["FF3"] = "%f"
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["FF0"] = "%f"
    # Timezone variants (TZH:TZM is most common, but TZHTZM and TZH also exist)
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["TZH:TZM"] = "%z"
    SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP["TZH"] = "%z"

    def __init__(self, format="yyyy-MM-dd", format_mode=None):
        """
        Parameters
        ----------

        format: str, optional
            Date format to use for string -> Date conversion if relevant. Uses either Spark date format,
            Snowflake date format, or Python/Pandas format depending on the value of format_mode.
        format_mode: DateFormat, optional
            the format mode to use, this allows the user to pick between Spark date format
            (https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html), Snowflake date format
            (https://docs.snowflake.com/en/sql-reference/date-time-input-output), and the native Python/Pandas date
            format. If not specified, uses the FLYPIPE_DEFAULT_DATE_FORMAT_MODE config (defaults to PYSPARK).
        """
        # Get default format mode from config if not specified
        if format_mode is None:
            from flypipe.config import get_config
            format_mode_str = get_config("default_date_format_mode")
            # Convert string to DateFormat enum (or use directly if already enum)
            if isinstance(format_mode_str, DateFormat):
                format_mode = format_mode_str
            else:
                # Get enum by value
                format_mode = DateFormat(format_mode_str)
        
        # Store the original format mode to give it preference when converting
        self._original_format_mode = format_mode
        
        if format_mode == DateFormat.PYSPARK:
            self._pyspark_format = format
            self._python_format = None
            self._snowflake_format = None
        elif format_mode == DateFormat.SNOWFLAKE:
            self._snowflake_format = format
            self._python_format = None
            self._pyspark_format = None
        else:
            self._python_format = format
            self._pyspark_format = None
            self._snowflake_format = None

    @property
    def pyspark_format(self):
        if not self._pyspark_format:
            # Always convert through python format
            self._pyspark_format = self.convert_python_to_pyspark_datetime_format(
                self.python_format
            )
        return self._pyspark_format

    @property
    def snowflake_format(self):
        if not self._snowflake_format:
            # Always convert through python format
            self._snowflake_format = self.convert_python_to_snowflake_datetime_format(
                self.python_format
            )
        return self._snowflake_format

    @property
    def python_format(self):
        if not self._python_format:
            # Prefer converting from the original format mode
            if self._original_format_mode == DateFormat.PYSPARK and self._pyspark_format:
                self._python_format = self.convert_pyspark_to_python_datetime_format(
                    self.pyspark_format
                )
            elif self._original_format_mode == DateFormat.SNOWFLAKE and self._snowflake_format:
                self._python_format = self.convert_snowflake_to_python_datetime_format(
                    self.snowflake_format
                )
            # Fallback: try any available format
            elif self._pyspark_format:
                self._python_format = self.convert_pyspark_to_python_datetime_format(
                    self.pyspark_format
                )
            elif self._snowflake_format:
                self._python_format = self.convert_snowflake_to_python_datetime_format(
                    self.snowflake_format
                )
        return self._python_format

    @classmethod
    def convert_python_to_pyspark_datetime_format(cls, python_format):
        pyspark_format = []
        while python_format:
            if python_format.startswith("%"):
                symbol = python_format[:2]
                python_format = python_format[2:]
                try:
                    pyspark_format.append(
                        cls.PYTHON_PYSPARK_DATETIME_SYMBOL_MAP[symbol]
                    )
                except KeyError as exc:
                    raise ValueError(
                        f"Unable to convert datetime symbol {symbol} to pyspark"
                    ) from exc
            else:
                formatting = python_format[0]
                python_format = python_format[1:]
                pyspark_format.append(formatting)
        return "".join(pyspark_format)

    @classmethod
    def convert_pyspark_to_python_datetime_format(cls, pyspark_format):
        python_format = []
        while pyspark_format:
            first_letter = pyspark_format[0]
            if first_letter.isalpha():
                i = 1
                while len(pyspark_format) > i and pyspark_format[i] == first_letter:
                    i += 1
                symbol = pyspark_format[:i]
                pyspark_format = pyspark_format[i:]
                try:
                    python_format.append(cls.PYSPARK_PYTHON_DATETIME_SYMBOL_MAP[symbol])
                except KeyError as exc:
                    raise ValueError(
                        f"Unable to convert datetime symbol {symbol} to python/pandas datetime format"
                    ) from exc
            else:
                formatting = pyspark_format[0]
                pyspark_format = pyspark_format[1:]
                python_format.append(formatting)
        return "".join(python_format)

    @classmethod
    def convert_python_to_snowflake_datetime_format(cls, python_format):
        snowflake_format = []
        while python_format:
            if python_format.startswith("%"):
                symbol = python_format[:2]
                python_format = python_format[2:]
                try:
                    snowflake_format.append(
                        cls.PYTHON_SNOWFLAKE_DATETIME_SYMBOL_MAP[symbol]
                    )
                except KeyError as exc:
                    raise ValueError(
                        f"Unable to convert datetime symbol {symbol} to snowflake"
                    ) from exc
            else:
                formatting = python_format[0]
                python_format = python_format[1:]
                snowflake_format.append(formatting)
        return "".join(snowflake_format)

    @classmethod
    def convert_snowflake_to_python_datetime_format(cls, snowflake_format):
        python_format = []
        while snowflake_format:
            first_letter = snowflake_format[0]
            if first_letter.isalpha():
                # Handle multi-character symbols like HH24, HH12, YYYY, TZH:TZM, etc.
                # Try to match the longest possible symbol first (up to 7 chars for TZH:TZM)
                matched = False
                for length in [7, 6, 5, 4, 3, 2, 1]:  # Try lengths from longest to shortest
                    if len(snowflake_format) >= length:
                        symbol = snowflake_format[:length]
                        if symbol in cls.SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP:
                            python_format.append(
                                cls.SNOWFLAKE_PYTHON_DATETIME_SYMBOL_MAP[symbol]
                            )
                            snowflake_format = snowflake_format[length:]
                            matched = True
                            break
                if not matched:
                    raise ValueError(
                        f"Unable to convert datetime symbol starting with '{first_letter}' to python/pandas datetime format"
                    )
            else:
                formatting = snowflake_format[0]
                snowflake_format = snowflake_format[1:]
                python_format.append(formatting)
        return "".join(python_format)


class DateTime(Date):
    """Flypipe datetime type"""

    def __init__(self, format="yyyy-MM-dd H:m:s", format_mode=None):
        """
        Parameters
        ----------

        format: str, optional
            Date format to use for string -> DateTime conversion if relevant. Uses either Spark datetime format,
            Snowflake datetime format, or Python/Pandas format depending on the value of format_mode.
        format_mode: DateFormat, optional
            the format mode to use, this allows the user to pick between Spark datetime format
            (https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html), Snowflake datetime format
            (https://docs.snowflake.com/en/sql-reference/date-time-input-output), and the native Python/Pandas datetime
            format. If not specified, uses the FLYPIPE_DEFAULT_DATE_FORMAT_MODE config (defaults to PYSPARK).
        """
        super().__init__(format=format, format_mode=format_mode)
