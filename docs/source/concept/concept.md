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

As each node (transformation) is connected to its ancessors, easily see the pipeline (`my_graph.html()`) or execute it
by invoking `my_graph.run()`

![Flypipe Graph Pipeline](../_static/images/flypipe_pipelines.svg)

## What Flypipe aims to facilitate?

- End-to-end transformations logic lineage
- Create development standards for data and machine learning engineers and data scientist
- Improve transformations re-usability in different pipelines
- Faster integration and portability of pipelines to non-spark APIS
- Low latency for on demand feature generation and predictions

## Other Flypipe advantages

* Break down feature transformation into smaller pieces.
  - Composeable transformations
  - Smaller transformations mean easier to reason about
  - Smaller transformations and injectable dependencies make unit testing easy
  - Injectable dependencies also make on the fly transformations possible
  - Graph allows for lineage visibility and far better high level table views over the pipeline
* Flexibility to use and mix up pyspark/pandas on spark/pandas in transformations
* Write once, use in variety of contexts
* Very lightweight, easily integratable into databricks, or can be used without databricks also
* Framework level optimisations and cool things like dynamic transformations