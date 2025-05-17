import inspect
from enum import Enum
from typing import Union, Callable

from flypipe.schema import Column

class WatermarkMode(Enum):
    DISABLE=1 # do not apply the watermark (if defined)

class Watermark:
    def __init__(self, func: Callable, column: Union[str, Column] = None):
        self.column = (
            column.name if column is not None and isinstance(column, Column) else column
        )
        self.func = func

    def apply(self, df):
        params = inspect.signature(self.func).parameters

        if len(params) == 1:
            return self.func(df)
        elif len(params) == 2:
            return self.func(df, self.column)
        else:
            raise ValueError("Function expects an unsupported number of arguments")

    def __repr__(self):
        params = inspect.signature(self.func).parameters
        if len(params) == 1:
            return f"Watermark(func={self.func}, column={self.column}) will call {self.func.__name__}(df)"
        elif len(params) == 2:
            return f"Watermark(func={self.func}, column={self.column}) will call {self.func.__name__}(df, '{self.column}')"
