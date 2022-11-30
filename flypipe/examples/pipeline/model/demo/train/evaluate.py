import pandas as pd

from flypipe import node
from flypipe.examples.pipeline.model.demo.train.train_svm_model import train_svm_model
from flypipe.schema import Schema, Column
from flypipe.schema.types import Float, String
from sklearn.metrics import f1_score


@node(
    type="pandas",
    description="Model training using SVM",
    tags=["model", "svm"],
    dependencies=[
        train_svm_model.select(
            'data_type',
            'target',
            'prediction'
        ).alias("df")
    ],
    output=Schema(
        Column('data_type', String(), 'all, train or test'),
        Column('metric', String(), 'score metric'),
        Column('value', Float(), 'value of the metric'),
    ))
def evaluate(df):
    result = pd.DataFrame(columns=['data_type', 'metric', 'value'])

    # All data
    score = f1_score(df['target'], df['prediction'], average='macro')
    result.loc[result.shape[0]] = ['all', 'f1_score macro', score]

    # Train data
    df_ = df[df['data_type'] == 'train']
    score = f1_score(df_['target'], df_['prediction'], average='macro')
    result.loc[result.shape[0]] = ['train', 'f1_score macro', score]

    # Test data
    df_ = df[df['data_type'] == 'test']
    score = f1_score(df_['target'], df_['prediction'], average='macro')
    result.loc[result.shape[0]] = ['test', 'f1_score macro', score]

    return result
