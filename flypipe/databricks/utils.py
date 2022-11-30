import os
import mlflow
from mlflow.exceptions import MlflowException


def mlflow_run(node, experiment_path, experiment_name, artifact_location):
    experiment_name = os.path.join(experiment_path, experiment_name)
    # Ends any actve mlflow run
    try:
        mlflow.end_run()
    except Exception as e:
        pass

    """
    Creates or gets an experiment from /Shared folder
    Sets the artifact location to the mounted blob
    """
    os.makedirs(artifact_location, exist_ok=True)

    try:
        experiment_id = mlflow.create_experiment(experiment_name,
                                                 artifact_location=artifact_location.replace("/dbfs/", "dbfs:/"))
    except MlflowException as e:
        print(e)
    finally:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id

    experiment = mlflow.get_experiment_by_name(experiment_name)
    print(f"Experiment: {experiment}")

    """
    Starts the mlflow run with the experiment
    """
    mlflow.start_run(experiment_id=experiment_id)
    RUN_ID = mlflow.active_run().info.run_id
    print(f"Active run_id: {RUN_ID}")

    df = node.run()
    mlflow.end_run()

    return df
