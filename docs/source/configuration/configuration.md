# Configuration

There are a number of configuration variables that can be set to control Flypipe behaviour at a global level. These can 
be set via [Environment Variables](#environment-variables) or via the [Context Manager](#context-manager) 

## Environment Variables

Below is a list of the available variables: 

**FLYPIPE_REQUIRE_NODE_DESCRIPTION**

Enforces declaration of node **description**
:type: boolean
:default: `False`

**FLYPIPE_REQUIRE_SCHEMA_DESCRIPTION**

Enforces declaration of node **output** schema
:type: boolean
:default: `False`

**FLYPIPE_DEFAULT_RUN_MODE**

Defines the default execution mode for Flypipe pipelines:

* **sequential**: will process nodes sequentially
* **parallel**: permit Flypipe to schedule multiple nodes to be processed concurrently, note that for a node to be 
processed all the usual rules about ancestors having already been executed will apply. 

:type: string
:default: `sequential`

**FLYPIPE_NODE_RUN_MAX_WORKERS**

Sets the maximum number of workers Flypipe will use when running transformations in parallel execution mode. 

:type: integer
:default: `os.cpu_count()`

```{note}
Beware- at the moment we don't anticipate parallel execution of nodes to be faster than sequential except for Pandas 
nodes running IO operations such as in datasource nodes. This is because most other node operations are CPU-bound, and 
Python only permits a single thread per process to execute Python bytecode. 
```

## Catalog

### catalog_count_box_tags

Which tags to show at the top of the Flypipe Catalog, seperated by commas. For each tag Flypipe will search through the 
nodes in the Catalog to obtain the number of nodes that have that tag. 

:type: string
:default: `bronze,silver,gold`

```{note}
If you are working in Databricks, you can configure environment variables for specific clusters 
(https://docs.databricks.com/clusters/configure.html#environment-variables). Commonly different teams will be using 
different clusters so you can easily setup different configurations by team with this approach.  
```

## Context Manager

Naturally when using the context manager the configuration will only persist for the code under the context. 
The environment variable map of a flypipe variable is always prefixed with FLYPIPE and uses uppercase. 

For example, to switch on the configuration `require_node_description` we can either set the environment variable 
FLYPIPE_REQUIRE_NODE_DESCRIPTION=True or in the code with: 

```
from flypipe.config import config_context

with config_context(require_node_description=True):
	...
```

Note that you can query the value of a configuration variable with the utility method `flypipe.config.get_config`. 


