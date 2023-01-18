# Flypipe

Flypipe is a Python framework to simplify development, management and maintainance of data, feature and ML model
pipelines.

Transformations are implemented in small pieces of code called `nodes` that are linked to each other in a DAG.

```python
from flypipe.node import node


@node(
  type="pandas",
  dependencies=[t0.select("fruit").alias("df")]
)
def t1(df):
  categories = {'mango': 'sweet', 'lemon': 'citric'}
  df['flavour'] = df['fruit']
  df = df.replace({'flavour': categories})
  return df
```

### Flypipe Pipelines

As each node (transformation) is connected to its ancestors, you can easily see the pipeline (`my_graph.html()`) or execute it
by invoking `my_graph.run()`

![Flypipe Graph Pipeline](./_static/images/flypipe_pipelines.svg)

## What Flypipe aims to facilitate?

- End-to-end logic lineage for transformations
- Create development standards for Data Engineers, Machine Learning Engineers and Data Scientists
- Improve re-usability of transformations in different pipelines
- Faster integration and portability of pipelines to non-spark APIs
- Low latency for on demand feature generation and predictions

## Other Flypipe advantages

* Break down feature transformations into smaller pieces.
    - Composable transformations
    - Numerous, smaller transformations are easier to reason about 
    - Smaller transformations doing a single operation and framework supported dependency injection make unit testing easy
    - Dependency injection also makes on the fly transformations possible
    - Graph allows for lineage visibility and far better high level table views over the pipeline
* Flexibility to use and mix up pyspark/pandas on spark/pandas/spark_sql in transformations
* Write once, use in a variety of contexts
* Very lightweight, easy to integrate into Databricks, or can be used without Databricks also
* Framework level optimisations and cool things like dynamic transformations