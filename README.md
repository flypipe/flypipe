# FlyPipe

## HOW IT WORKS

### Nodes

**Parameters**:

- mode (default `pypsark`): defines the type of the inputs
   - pyspark: convert every argument to pyspark dataframe
   - pandas: convert every argument to a pandas dataframe
   - pandas_on_spark: convert every argument to a Pandas on Spark dataframe (see [Pandas API](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html) and [this blog](https://www.databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html))
   - spark_sql: convert every argument to a view to be used in a [Spark Sql](#Spark-SQL-Transformation)
- cache (default `None`): caches the **inputs** dataframes
   - `Flypipe.Cache`: defines the method you want to [Cache](#cache)
- inputs: List of other node transformations or a [datasources](#datasources)
- output: defines the schema of the node transformation
- persist (default `None`): saves the output dataframe from the transformation
  - `DataSource`: datasource to be [saved](#save)

### Node Cache

Caches or retrives the result of previous node.

Sometimes, usually when developing a transformation, you don't want to re-process all input nodes every 
time, for example, the node [number_users_per_state](#Spark-SQL-Transformation) imports
[user_state](#PySpark-Transformation) that imports a spark table `raw.user_account`.

As you can see, in the decorator of [number_users_per_state](#Spark-SQL-Transformation)
the cache is activated `cache=Cache(CacheType.DISK)`.

It will cause caching of immediate predecessors of the node (`user_state`) and a table with results 
`flypipe.cache_user_state` of this node be created.

 user_id | state |
--- | --- |
jhon_foo | AL |
jhon_foo | AK |
... | ... |
jhon_foo | AR |

Next time that you trigger the execution of the `number_users_per_state` instead of executing the following
graph `raw.user_account -> user_state -> number_users_per_state`, it will execute query directly the results of
`flypipe.cache_user_state` and execute `number_users_per_state`

:warning: This is an expensive operation as it persists data to disk, use caching with caution.

:::TODO:::

### Datasources

:::TODO:::

#### Load

:::TODO:::

#### Save

:::TODO:::

## Development

### Workstation
We setup a development environment running with a Spark and Hive server to manages Spark metastore together with 
JupyterLab.

We want to develop in an environment that simulates popular Analytics platform like Databricks. For this reason, we
setup JupyterLab to simulate databricks. 

The following instructions builds and starts up 6 dockers:
- jupyter: runs jupyter notebooks and spark
- hive-server, hive-metastore, hive-metastore-postgresql, datanode and namenode: builds and deploys hive-server, 
- more info see https://hshirodkar.medium.com/apache-hive-on-docker-4d7280ac6f8e

#### 1. Building and starting the development environment
 
- build `docker-compose build`
- run `docker-compose up`
- Access JupyterLab http://127.0.0.1:8888/lab

#### Cleaning local environment

- Delete docker files: delete folders `/data`, `/hdfs`, `/metastore-postgresql`, `spark-warehouse`
- Remove images `flypipe_jupyter`, `bde2020/hive-metastore-postgresql`, `bde2020/hadoop-datanode`, `bde2020/hadoop-namenode` and `bde2020/hive`

#### 2. Recreating development database
For development purpose it is nice to have some data in the spark warehouse to develop Flypipe.
We included a notebook `build_database.ipynb` that downloads Yellow taxi trips from New York City
(~ 9 millions rows)

- Open and run the notebook `build_db/build_database.ipynb`
- Test if data can be query the data wihtin another spark session by running `build_db/test_loading_data.ipynb`

Obs.: you can download more data if needed, you can download the data here 
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page


## Other useful commands

**Access Jupyter docker**

```
docker run -p 8090:8090 -v notebooks:/notebooks -it --entrypoint "" flypipe_jupyter bash
```

**Run Jupyter notebooks within docker**

```
jupyter notebook --notebook-dir /notebooks --allow-root --no-browser --port 8090 --ip 0.0.0.0
```

## Examples

### PySpark Transformation

```python
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
```

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