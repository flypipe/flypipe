from enum import Enum


class RunStatus(Enum):
    """Describes the run state of a node in a pipeline when that pipeline is executed"""

    UNKNOWN = 0
    ACTIVE = 1
    SKIP = 2
    CACHED = 3
