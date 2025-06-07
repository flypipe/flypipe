from enum import Enum


class PreprocessMode(Enum):
    DISABLE = 1  # do not apply the preprocess function (if defined)
    ACTIVE = 2  # by default run the Preprocess
