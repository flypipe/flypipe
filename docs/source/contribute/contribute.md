## How to contribute

For the full list of contributors, take a look at {{project}}'s [GitHub Contributor](https://github.com/flypipe/flypipe/graphs/contributors) page.


We setup a development environment running with a Spark and Hive server to manages Spark metastore together with 
JupyterLab.

We want to develop in an environment that simulates popular Analytics platform like Databricks. For this reason, we
setup JupyterLab to simulate databricks. 

The following instructions builds and starts up 6 dockers:
- jupyter: runs jupyter notebooks and spark
- hive-server, hive-metastore, hive-metastore-postgresql, datanode and namenode: builds and deploys hive-server, 
- more info see https://hshirodkar.medium.com/apache-hive-on-docker-4d7280ac6f8e

### 1. Building and starting the development environment
 
- build `docker-compose build`
- run `docker-compose up`
- Access JupyterLab http://127.0.0.1:8888/lab

#### Cleaning local environment

- Delete docker files: delete folders `/data`, `/hdfs`, `/metastore-postgresql`, `spark-warehouse`
- Remove images `flypipe_jupyter`, `bde2020/hive-metastore-postgresql`, `bde2020/hadoop-datanode`, `bde2020/hadoop-namenode` and `bde2020/hive`

### 2. Recreating development database
For development purpose it is nice to have some data in the spark warehouse to develop {{project}}.
We included a notebook `build_database.ipynb` that downloads Yellow taxi trips from New York City
(~ 9 millions rows)

- Open and run the notebook `build_db/build_database.ipynb`
- Test if data can be query the data wihtin another spark session by running `build_db/test_loading_data.ipynb`

Obs.: you can download more data if needed, you can download the data here 
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Rebuilding documents

from `/flypipe` run `sh docs/build_docs.sh` (note you will need to have the python dependencies installed via a virtual env or globally)

### Other useful commands

**Access Jupyter docker**

```
docker run -p 8090:8090 -v notebooks:/notebooks -it --entrypoint "" flypipe_jupyter bash
```

**Run Jupyter notebooks within docker**

```
jupyter notebook --notebook-dir /notebooks --allow-root --no-browser --port 8090 --ip 0.0.0.0
```

