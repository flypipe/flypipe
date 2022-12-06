# Flypipe

Flypipe is a Python framework to simplify development, management and maintainance of data and feature pipelines.

## What Flypipe aims to solve?

TODO

## Flypipe advantages

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