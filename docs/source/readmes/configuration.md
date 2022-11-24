This page contains the list of all the available Flypipe configurations that you can set using environment variables.

[Governance](#Governance)
: [FLYPIPE_REQUIRE_NODE_DESCRIPTION](#FLYPIPE_REQUIRE_NODE_DESCRIPTION)
: [FLYPIPE_REQUIRE_SCHEMA_DESCRIPTION](#FLYPIPE_REQUIRE_SCHEMA_DESCRIPTION)

[Execution](#Execution)
: [FLYPIPE_DEFAULT_RUN_MODE](#tFLYPIPE_DEFAULT_RUN_MODE)

## Governance

### FLYPIPE_REQUIRE_NODE_DESCRIPTION

Enforces declaration of node **description**
:type: string
:default: `false`

#### FLYPIPE_REQUIRE_SCHEMA_DESCRIPTION

Enforces declaration of node **output** schema
:type: string
:default: `false`

## Execution

### FLYPIPE_DEFAULT_RUN_MODE

defines the default mode to execute the graph:\

* **sequential**: will process nodes sequentially
* **parallel**: will process nodes in parallel

:type: string
:default: `seqential`

```{note} 
Flypipe will behave differently depending on 
the [environment variables](https://docs.databricks.com/clusters/configure.html#environment-variables)
set on the Databricks Cluster you are using.\
If you have different clusters to different teams, you can use Flypipe settings
to behave differently for each team, for example, enforce more restrict governance for teams using a specific cluster. 

```
