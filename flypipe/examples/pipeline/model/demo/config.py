import pandas as pd

from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String

_config = {
    "artifact_location": "/tmp/flypipe_demo/artifacts/",
    "experiment_name": "flypipe_demo",
    "experiment_path": "/Shared",
    "production_run_id": "f1d5f975356e4565a87a37019460f026",
}


@node(
    type="pandas",
    output=Schema(
        Column(
            "artifact_location",
            String(),
            f"artifacts path location (default `{_config['artifact_location']}`)",
        ),
        Column(
            "experiment_name",
            String(),
            f"name of the mlflow experiment (default `{_config['experiment_name']}`)",
        ),
        Column(
            "experiment_path",
            String(),
            f"path of the experiment (default `{_config['experiment_path']}`)",
        ),
        Column(
            "production_run_id",
            String(),
            f"production run id (default `{_config['production_run_id']}`)",
        ),
    ),
)
def config():
    """
    demo model configurations
    """
    return pd.DataFrame(data=[_config])
