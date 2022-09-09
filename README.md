# FlyPipe

## HOW IT WORKS

### Nodes

**Parameters**:

- mode (default `pypsark`): defines the type of the inputs
   - pyspark: convert every argument to pyspark dataframe
   - pandas: convert every argument to a pandas dataframe
   - pandas_on_spark: convert every argument to a Pandas on Spark dataframe (see [Pandas API](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html) and [this blog](https://www.databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html))
   - spark_sql: convert every argument to a view to be used in a [Spark Sql](#spark-sql-transformation)
- cache (default `None`): caches the **inputs** dataframes
   - `Flypipe.Cache`: defines the method you want to [Cache](#node-cache)
- inputs: List of other node transformations or a [datasources](#datasources)
- output: defines the schema of the node transformation
- persist (default `None`): saves the output dataframe from the transformation
  - `DataSource`: datasource to be [saved](#save)

### Node Cache

Caches or retrives the result of previous node.

Sometimes, usually when developing a transformation, you don't want to re-process all input nodes every 
time, for example, the node [number_users_per_state](#spark-sql-transformation) imports
[user_state](#pyspark-transformation) that imports a spark table `raw.user_account`.

As you can see, in the decorator of [number_users_per_state](#spark-sql-transformation)
the cache is activated `cache=Cache(CacheType.DISK)`.

It will cause caching of immediate predecessors of the node (`user_state`) and a table with results 
`flypipe.cache_user_state` of this node be created.

| user_id | state |
| --- | --- |
| jhon_foo | AL |
| jhon_foo | AK |
| ... | ... |
| jhon_foo | AR |

Next time that you trigger the execution of the `number_users_per_state` instead of executing the following
graph `raw.user_account -> user_state -> number_users_per_state`, it will execute query directly the results of
`flypipe.cache_user_state` and execute `number_users_per_state`

:warning: This is an expensive operation as it persists data to disk, use caching with caution.

TODO

### Datasources

TODO

#### Load

TODO

#### Save

TODO

## Examples

### PySpark Transformation

````python
from flypipe import node
from flypipe import Spark
from flypipe.converter import Schema, StringType
import pyspark.sql.functions as F


@node(mode="pyspark",
  inputs=[Spark("raw.user_account", columns=["user_key", "address_state"])],
  outputs=[Schema("user_id", StringType(), primary_key=True),
           Schema("state", StringType())])
def user_state(raw_user_account):
  df = raw_user_account
  df = df.withColumnRenamed("user_key", "user_id")
  return df.withColumn("state", F.upper(F.col("address_state")))
````

### Spark SQL Transformation

```python
from flypipe import node
from flypipe import Cache, CacheType
from my_nodes import user_state
from flypipe.converter import Schema, StringType, IntegerType


@node(mode="spark_sql",
  inputs=[user_accounts],
  cache=Cache(CacheType.DISK),
  outputs=[Schema("state", StringType(), primary_key=True),
           Schema("quantity", IntegerType())])
def number_users_per_state(user_states):
  return f"""
        SELECT
            state,
            count(user_id) as quantity            
        FROM
            {user_accounts}
        GROUP BY 
            state
        """
```