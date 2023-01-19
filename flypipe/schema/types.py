from flypipe.schema.util import DateFormat


class Type:
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
    pass


class Boolean(Type):
    VALID_VALUES = {True, False, 1, 0}

    @property
    def valid_values(self):
        return self.VALID_VALUES


class Byte(Type):
    pass


class Binary(Type):
    pass


class Integer(Type):
    pass


class Short(Type):
    pass


class Long(Type):
    pass


class Float(Type):
    pass


class Double(Type):
    pass


class String(Type):
    pass


class Decimal(Type):
    def __init__(self, precision: int = 13, scale: int = 2):
        self.precision = precision
        self.scale = scale


class Date(Type):
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

    def __init__(self, format="yyyy-MM-dd", format_mode=DateFormat.PYSPARK):
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
                except KeyError:
                    raise ValueError(
                        f"Unable to convert datetime symbol {symbol} to pyspark"
                    )
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
                except KeyError:
                    raise ValueError(
                        f"Unable to convert datetime symbol {symbol} to python/pandas datetime format"
                    )
            else:
                formatting = pyspark_format[0]
                pyspark_format = pyspark_format[1:]
                python_format.append(formatting)
        return "".join(python_format)


class DateTime(Date):
    def __init__(self, format="yyyy-MM-dd H:m:s", format_mode=DateFormat.PYSPARK):
        super().__init__(format=format, format_mode=format_mode)
