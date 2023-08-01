## How to contribute

We setup a development environment running with a Spark and Hive server to manages Spark metastore together with 
JupyterLab.

We want to develop in an environment that simulates popular Analytics platform like Databricks. For this reason, we
setup JupyterLab to simulate databricks. 

### 1. Building and starting the development environment
 
`make up`
Access JupyterLab http://127.0.0.1:8888/lab

#### Cleaning local environment

`make down`

### Rebuilding documents

`make docs`

### Other useful commands

**Access Jupyter docker**

`make jupyter-bash`

**pylint**

`make pylint`

**coverage**

Runs unit tests and coverage

`make coverage`

**black**

`make black`

**check mariadb and hivestore are live**

`make ping`

You should see:

```
docker compose -f ./local/docker-compose.yaml run --entrypoint "" flypipe-jupyter ./wait-for-it.sh -h flypipe-mariadb -p 3306
wait-for-it.sh: waiting 15 seconds for flypipe-mariadb:3306
wait-for-it.sh: flypipe-mariadb:3306 is available after 0 seconds
docker compose -f ./local/docker-compose.yaml run --entrypoint "" flypipe-jupyter ./wait-for-it.sh -h flypipe-hive-metastore -p 9083
wait-for-it.sh: waiting 15 seconds for flypipe-hive-metastore:9083
wait-for-it.sh: flypipe-hive-metastore:9083 is available after 0 seconds
```