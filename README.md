# Flypipe

[![verification](https://github.com/flypipe/flypipe/actions/workflows/verification.yml/badge.svg?branch=main)](https://github.com/flypipe/flypipe/actions/workflows/verification.yml)

Flypipe is a Python framework to simplify development, management and maintenance of transformation pipelines, which are 
commonly used in the data, feature and ML model space.

Each transformation is implemented in a small, composable function, a special decorator is then used to define it as a 
Flypipe node, which is the primary model Flypipe uses. Metadata on the node decorator allows for multiple nodes to be 
linked together into a Directed Acyclic Graph (DAG). 

```python
from flypipe.node import node


@node(
  type="pandas",
  dependencies=[t0.select("fruit").alias("df")]
)
def t1(df):
  categories = {'mango': 'sweet', 'lemon': 'sour'}
  df['flavour'] = df['fruit']
  df = df.replace({'flavour': categories})
  return df
```

### Flypipe Pipelines

As each node (transformation) is connected to its ancestors, we can easily view the pipeline graphically in a html page 
(`my_graph.html()`) or execute it by invoking `my_graph.run()`

![Flypipe Graph Pipeline](/docs/source/_static/images/flypipe_pipelines.png)

## What Flypipe aims to facilitate?

- Free open-source tool for data transformations
- Facilitate streaming pipeline development (improved use of caches)  
- Increase pipeline stability (better use of unittests)
- End-to-end transformation lineage
- Create development standards for Data Engineers, Machine Learning Engineers and Data Scientists
- Improve re-usability of transformations in different pipelines & contexts via composable nodes
- Faster integration and portability of pipelines to different contexts with different available technology stacks:
  - Flexibility to use and mix up pyspark/pandas on spark/pandas in transformations seamlessly
  - As a simple wheel package, it's very lightweight and unopinionated about runtime environment. This allows for it to 
  be easily integrated into Databricks and independently of Databricks. 
- Low latency for on-demand feature generation and predictions
- Framework level optimisations and dynamic transformations help to make even complex transformation pipelines low 
latency. This in turn allows for on-demand feature generation/predictions.

## Commonly used

<p float="left">
  <img src="./docs/source/_static/images/databricks_logo.png" alt="Databricks" style="height:100px;"/>
  <img src="./docs/source/_static/images/python.png" alt="Python" style="height:80px;"/>
</p>

## Source Code

API code is available at [https://github.com/flypipe/flypipe](https://github.com/flypipe/flypipe).

## Documentation

Full documentation is available at [https://flypipe.github.io/flypipe/](https://flypipe.github.io/flypipe/).