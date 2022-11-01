class Type:

    @classmethod
    def key(cls):
        return cls.__name__.lower()

    @property
    def name(self):
        return self.__class__.__name__

    @classmethod
    def is_valid_value(cls, value):
        return True


class Unknown(Type):
    pass


class Boolean(Type):
    VALID_VALUES = {True, False, 1, 0}

    @classmethod
    def is_valid_value(cls, value):
        return value in cls.VALID_VALUES


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

    def __init__(self, fmt: str = "%Y-%m-%d"):
        self.fmt = fmt


class DateTime(Date):

    def __init__(self, fmt: str = "%Y-%m-%d %H:%M:%S"):
        self.fmt = fmt
        super().__init__(fmt=self.fmt)
