# Configuration Reference

There are a number of configuration variables that can be set to control Flypipe behaviour at a global level. These can 
be set via environment variables or via the context manager from `flypipe.config.config_context` (Naturally 
when using the context manager the configuration will only persist for the code under the context). The environment 
variable map of a flypipe variable is always prefixed with FLYPIPE and uses uppercase. 

For example, to switch on the configuration `require_node_description` we can either set the environment variable 
FLYPIPE_REQUIRE_NODE_DESCRIPTION=True or in the code with: 

```
from flypipe.config import config_context

with config_context(require_node_description=True):
	...
```

Note that you can query the value of a configuration variable with the utility method flypipe.config.get_config. 

Below is a list of the available configuration variables: 

[Governance](#Governance)

* [require_node_description](#require_node_description)
* [require_schema_description](#require_schema_description)

[Execution](#Execution)

* [default_run_mode](#default_run_mode)

## Governance

### require_node_description

Enforces declaration of node **description**
:type: boolean
:default: `False`

### require_schema_description

Enforces declaration of node **output** schema
:type: boolean
:default: `False`

## Execution

### default_run_mode

Defines the default execution mode for Flypipe pipelines:

* **sequential**: will process nodes sequentially
* **parallel**: permit Flypipe to schedule multiple nodes to be processed concurrently, note that for a node to be 
processed all the usual rules about ancestors having already been executed will apply. 

:type: string
:default: `sequential`

```{note}
If you are working in Databricks, you can configure environment variables for specific clusters 
(https://docs.databricks.com/clusters/configure.html#environment-variables). Commonly different teams will be using 
different clusters so you can easily setup different configurations by team with this approach.  
```
