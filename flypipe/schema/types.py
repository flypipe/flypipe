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


class Unknown(Type):
    """Special type that is used when the type of column is unknown"""


class Boolean(Type):
    """Flypipe boolean type"""

    VALID_VALUES = {True, False, 1, 0}  # pylint: disable=duplicate-value

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

    def __init__(
        self, format="yyyy-MM-dd", format_mode=DateFormat.PYSPARK
    ):  # pylint: disable=redefined-builtin
        """
        Parameters
        ----------

        format: str, optional
            Date format to use for string -> Date conversion if relevant. Uses either Spark date format or
            Python/Pandas format depending on the value of format_mode.
        format_mode: DateFormat, optional
            the format mode to use, this allows the user to pick between Spark date format
            (https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html) and the native Python/Pandas date
            format.
        """
        if format_mode == DateFormat.PYSPARK:
            self._pyspark_format = format
            self._python_format = None
        else:
            self._python_format = format
            self._pyspark_format = None

    @property
    def pyspark_format(self):
        if not self._pyspark_format:
            self._pyspark_format = self.convert_python_to_pyspark_datetime_format(
                self.python_format
            )
        return self._pyspark_format

    @property
    def python_format(self):
        if not self._python_format:
            self._python_format = self.convert_pyspark_to_python_datetime_format(
                self.pyspark_format
            )
        return self._python_format

    @classmethod
    def convert_python_to_pyspark_datetime_format(cls, python_format):
        pyspark_format = []
        while python_format:
            if python_format.startswith("%"):
                symbol = python_format[:2]
                print(f'Extracted symbol "{symbol}"')
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
                print(f'Extracted formatting "{formatting}"')
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


class DateTime(Date):
    """Flypipe datetime type"""

    def __init__(
        self, format="yyyy-MM-dd H:m:s", format_mode=DateFormat.PYSPARK
    ):  # pylint: disable=redefined-builtin
        """
        Parameters
        ----------

        format: str, optional
            Date format to use for string -> DateTime conversion if relevant. Uses either Spark datetime format or
            Python/Pandas format depending on the value of format_mode.
        format_mode: DateFormat, optional
            the format mode to use, this allows the user to pick between Spark datetime format
            (https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html) and the native Python/Pandas datetime
            format.
        """
        super().__init__(format=format, format_mode=format_mode)
