Flypipe is a Python framework to simplify development, management and maintainance of data and feature pipelines.

## What Flypipe aims to solve?

TODO

### Nodes

```{eval-rst}  
.. autoclass:: flypipe.node
```

### Node Cache

Caches or retrives the result of previous node.

Sometimes, usually when developing a transformation, you don't want to re-process all input nodes every time, for
example, the node [number_users_per_state](#spark-sql-transformation) imports
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


@node(type="pyspark",
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


@node(type="spark_sql",
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