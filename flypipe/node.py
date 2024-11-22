import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from pyspark.sql import SparkSession

from flypipe.cache.cache import Cache
from flypipe.config import get_config
from flypipe.node_input import InputNode
from flypipe.node_run_context import NodeRunContext
from flypipe.node_type import NodeType
from flypipe.run_context import RunContext
from flypipe.run_status import RunStatus
from flypipe.schema import Schema, Column
from flypipe.schema.types import Unknown
from flypipe.utils import DataFrameType

logger = logging.getLogger(__name__)


class Node:
    """
    Central model for Flypipe. Should be used indirectly through the `node` decorator rather than directly referencing
    it.
    """

    ALLOWED_TYPES = {"pyspark", "pandas", "pandas_on_spark", "spark_sql"}
    DATAFRAME_TYPE_MAP = {
        "pyspark": DataFrameType.PYSPARK,
        "pandas": DataFrameType.PANDAS,
        "pandas_on_spark": DataFrameType.PANDAS_ON_SPARK,
        "spark_sql": DataFrameType.PYSPARK,
    }

    def __init__(
        self,
        function,
        type: str,
        description: str = None,
        group: str = None,
        tags: List[str] = None,
        dependencies: List[InputNode] = None,
        output: Schema = None,
        spark_context: bool = False,
        requested_columns: List[str] = False,
        cache: Cache = None,
    ):
        self._key = None
        self.name = None
        self.function = function

        self.node_type = NodeType.TRANSFORMATION
        if type not in self.ALLOWED_TYPES:
            raise ValueError(
                f"type set to {type} but must be one of {self.ALLOWED_TYPES}"
            )
        self.type = type

        if description:
            self.description = description
        elif self.function.__doc__:
            self.description = self.function.__doc__.strip()
        else:
            if get_config("require_node_description"):
                raise ValueError(
                    f"Node description configured as mandatory but no description provided for node {self.__name__}"
                )
            self.description = ""

        self.group = group

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.tags = []
        if tags:
            self.tags.extend(tags)

        self.input_nodes = self._get_input_nodes(dependencies)

        self._provided_inputs = {}

        # TODO: enforce tags for now, later validation can be set as optional via environment variable
        self.output_schema = output

        self.spark_context = spark_context
        self.requested_columns = requested_columns
        self.node_graph = None

        if cache is not None and not isinstance(cache, Cache):
            raise TypeError("cache is not of type flypipe.cache.Cache")
        self.cache = cache

    @property
    def output(self):
        return self.output_schema

    def _get_input_nodes(self, dependencies):
        input_nodes = []
        input_node_keys = set()
        input_node_alias = set()
        if dependencies is None:
            dependencies = []
        for dependency in dependencies:
            if dependency.key in input_node_keys:
                raise ValueError(
                    f"Illegal operation- node {self.__name__} is using the same node {dependency.__name__} more than "
                    f"once"
                )

            if isinstance(dependency, Node):
                input_node = InputNode(dependency)
                input_nodes.append(input_node)
            elif isinstance(dependency, InputNode):
                input_node = dependency
                input_nodes.append(input_node)
            else:
                raise ValueError(
                    f"Expected all dependencies of node {self.__name__} to be of format node/node.alias(...)/node."
                    f"select(...) but received {dependency} of type {type(dependency)}"
                )

            if input_node.get_alias() in input_node_alias:
                raise ValueError(
                    f"Illegal operation- node {self.__name__} has multiple nodes with the same name/alias"
                )
            input_node_keys.add(input_node.key)
            input_node_alias.add(input_node.get_alias())
        return input_nodes

    @property
    def __name__(self):
        if hasattr(self, "name") and self.name:
            return self.name
        return self.function.__name__

    @property
    def __class__(self):
        return self.function.__class__

    @property
    def __package__(self):
        # When running a pipeline of node declared in the same
        # notebook, it throws an error as it not finds __package
        # in that case, returns nothing
        if hasattr(sys.modules[self.function.__module__], "__package"):
            return sys.modules[self.function.__module__].__package__
        return None

    @property
    def __file__(self):
        # When running a pipeline of node declared in the same
        # notebook, it throws an error as it not finds __file__
        # in that case, returns nothing
        if hasattr(sys.modules[self.function.__module__], "__file__"):
            return sys.modules[self.function.__module__].__file__
        return None

    @property
    def __module__(self):
        return self.function.__module__

    @property
    def key(self):
        """
        Generate a key for a node for use in dictionaries, etc. The main goal is for it to be unique, so that nodes
        with the same function name still return different keys.
        """
        if self._key is None:
            key = (
                f"{self.function.__module__}.{self.function.__class__.__name__}.{self.function.__name__}."
                f"{self.function.__qualname__}"
            )
            self._key = re.sub(r"[^\da-zA-Z]", "_", key)
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def __doc__(self):
        """Return the docstring of the wrapped transformation rather than the docstring of the decorator object"""
        return self.function.__doc__

    def create_graph(self, run_context: RunContext):
        # This import is here to avoid a circular import issue
        from flypipe.node_graph import NodeGraph

        self.node_graph = NodeGraph(self, run_context=run_context)

    def select(self, *columns):
        return InputNode(self).select(*columns)

    def alias(self, value):
        return InputNode(self).alias(value)

    def get_node_inputs(self, run_context: RunContext):
        inputs = {}
        for input_node in self.input_nodes:
            node_input_value = run_context.node_results[input_node.key].as_type(
                self.dataframe_type
            )
            if input_node.selected_columns:
                node_input_value = node_input_value.select_columns(
                    *input_node.selected_columns
                )
            alias = input_node.get_alias()
            inputs[alias] = node_input_value.get_df()
            if self.type == "spark_sql":
                # SQL doesn't work with dataframes, so we need to:
                # - save all incoming dataframes as unique temporary tables
                # - pass the names of these tables instead of the dataframes
                table_name = f"{self.__name__}__{alias}"
                inputs[alias].createOrReplaceTempView(table_name)
                inputs[alias] = table_name

        return inputs

    def __call__(self, *args):
        return self.function(*args)

    def run(
        self,
        spark: SparkSession = None,
        parallel: bool = None,
        inputs: dict = None,
        pandas_on_spark_use_pandas: bool = False,
        parameters: dict = None,
        cache: dict = None,
    ):
        if not inputs:
            inputs = {}

        run_context = RunContext(
            spark=spark,
            parallel=parallel,
            provided_inputs=inputs,
            pandas_on_spark_use_pandas=pandas_on_spark_use_pandas,
            parameters=parameters,
            cache_modes=cache,
        )

        self.create_graph(run_context)
        execution_graph = self.node_graph.get_execution_graph(run_context)

        if run_context.parallel:
            self._run_parallel(run_context, execution_graph)
        else:
            self._run_sequential(run_context, execution_graph)

        end_node_name = self.node_graph.get_end_node_name(self.node_graph.graph)
        end_node = self.node_graph.get_transformation(end_node_name)
        return (
            run_context.node_results[end_node_name]
            .as_type(end_node.dataframe_type)
            .get_df()
        )

    @property
    def dataframe_type(self):
        return self.DATAFRAME_TYPE_MAP[self.type]

    def _run_parallel(self, run_context: RunContext, execution_graph):
        def execute(node):
            self.process_transformation_with_cache(node, run_context)
            return node["transformation"].key

        logger.info("Starting parallel processing of node %s", node.__name__)
        with ThreadPoolExecutor(
            max_workers=get_config("node_run_max_workers")
        ) as executor:
            visited = set()
            jobs = set()
            initial_nodes_to_run = [
                runnable_node
                for runnable_node in execution_graph.get_runnable_transformations()
                if runnable_node["transformation"].key not in run_context.node_results
            ]
            for runnable_node in initial_nodes_to_run:
                logger.info(
                    "Schedule parallelised execution of node %s",
                    runnable_node["transformation"].__name__,
                )
                jobs.add(executor.submit(execute, runnable_node))
                visited.add(runnable_node["transformation"].key)
            while jobs:
                to_remove = set()
                for job in as_completed(jobs):
                    # When we finish processing a node we remove it from the execution graph and check if there are any
                    # new eligible nodes to be run.
                    processed_node_key = job.result()
                    to_remove.add(job)
                    execution_graph.remove_node(processed_node_key)
                    runnable_nodes = execution_graph.get_runnable_transformations()
                    for runnable_node in runnable_nodes:
                        node_key = runnable_node["transformation"].key
                        if (
                            node_key not in visited
                            and node_key not in run_context.node_results
                        ):
                            logger.info(
                                "Schedule parallelised execution of node %s",
                                runnable_node["transformation"].__name__,
                            )
                            jobs.add(executor.submit(execute, runnable_node))
                            visited.add(runnable_node["transformation"].key)
                jobs = jobs - to_remove

    def _run_sequential(self, run_context: RunContext, execution_graph):
        while not execution_graph.is_empty():
            runnable_nodes = execution_graph.get_runnable_transformations()
            for runnable_node in runnable_nodes:

                execution_graph.remove_node(runnable_node["transformation"].key)

                if runnable_node["transformation"].key in run_context.node_results:
                    continue

                self.process_transformation_with_cache(runnable_node, run_context)

    @classmethod
    def _get_consolidated_output_schema(cls, output_schema, output_columns):
        """
        The output schema for a transformation is currently optional. If not provided, we create a simple one from the
        set of columns selected by descendant nodes.
        """
        if output_schema:
            schema = output_schema
        elif output_columns is not None:
            columns = []
            for output_column in output_columns:
                columns.append(Column(output_column, Unknown(), ""))
            schema = Schema(columns)
        else:
            schema = None
        return schema

    def process_transformation_with_cache(self, runnable_node, run_context: RunContext):

        result = None
        node_transformation = runnable_node["transformation"]
        ran_transformation = False

        if (
            runnable_node["status"] == RunStatus.CACHED
            and runnable_node["node_run_context"].cache_context.exists_cache_to_load
        ):
            result = runnable_node["node_run_context"].cache_context.read()
        else:
            ran_transformation = True
            dependency_values = runnable_node["transformation"].get_node_inputs(
                run_context
            )

            result = node_transformation.process_transformation(
                run_context.spark,
                runnable_node["output_columns"],
                runnable_node["node_run_context"],
                **dependency_values,
            )

        run_context.update_node_results(
            node_transformation.key,
            result,
            schema=self._get_consolidated_output_schema(
                node_transformation.output_schema,
                runnable_node["output_columns"],
            ),
        )

        if ran_transformation:
            # If cache exists, and the transformation has run, then save its cache
            runnable_node["node_run_context"].cache_context.write(
                run_context.node_results[node_transformation.key]
                .as_type(node_transformation.dataframe_type)
                .get_df()
            )

    def process_transformation(
        self, spark, requested_columns: list, node_run_context: NodeRunContext, **inputs
    ):
        # TODO: apply output validation + rename function to transformation, select only necessary columns specified in
        # self.dependencies_selected_columns
        parameters = inputs
        if self.spark_context:
            parameters["spark"] = spark

        if self.requested_columns:
            parameters["requested_columns"] = requested_columns

        if node_run_context.parameters:
            parameters = {**parameters, **node_run_context.parameters}

        result = self.function(**parameters)
        if self.type == "spark_sql":
            # Spark SQL functions only return the text of a SQL query, we will need to execute this command.
            if not spark:
                raise ValueError(
                    "Unable to run spark_sql type node without spark being provided in the transformation.run call"
                )
            result = spark.sql(result)

        return result

    def html(
        self,
        spark=None,
        height=1000,
        inputs=None,
        pandas_on_spark_use_pandas=False,
        parameters=None,
        cache=None,
    ):
        """
        Retrieves html string of the graph to be executed.

        Parameters
        ----------

        width : int, default None
            viewport width in pixels
        height : int, default 1000
            viewport height in pixels
        inputs : dict, default None
            dictionary where keys are Nodes and values dataframes, these dataframes will skip the nodes executions as
            they have been provided
        pandas_on_spark_use_pandas : bool, default False
            If True, convert and runs `pandas_on_spark` as `pandas`
        parameters : dict, default None
            dictionary dict(Node,dict(str,obj)) of parameters to be given to the nodes when executing them.

        Returns
        -------
        str
            html of the graph

        """

        # This import needs to be here to avoid a circular import issue (graph_html -> node_graph -> imports node)
        from flypipe.catalog import Catalog

        catalog = Catalog(spark=spark)
        catalog.register_node(
            self,
            inputs=inputs,
            pandas_on_spark_use_pandas=pandas_on_spark_use_pandas,
            parameters=parameters,
            cache=cache,
            add_node_to_graph=True,
        )
        return catalog.html(height)

    def __eq__(self, other):
        return self.key == other.key

    def __hash__(self):
        return hash(self.key)

    def copy(self):
        # Note this is a DEEP copy and will copy all ancestor nodes by extension
        node = Node(
            self.function,
            self.type,
            group=self.group,
            description=self.description,
            tags=list(self.tags),
            dependencies=[input_node.copy() for input_node in self.input_nodes],
            output=None if self.output_schema is None else self.output_schema.copy(),
            spark_context=self.spark_context,
            requested_columns=self.requested_columns,
            cache=self.cache,
        )
        node.name = self.name
        # Accessing protected members in a deep copy method is necessary
        node._key = self._key
        node.node_type = self.node_type
        return node


def node(type, *args, **kwargs):
    """
    Nodes are the fundamental building block of Flypipe. Simply apply the node function as a decorator to a
    transformation function in order to declare the transformation as a Flypipe node.

    Parameters
    ----------

    type : str
            Type of the node transformation "pandas", "pandas_on_spark", "pyspark", "spark_sql"
    description : str, optional
        Description of the node (default is None)
    group : str, optional
        Group the node falls under, nodes in the same group are clustered together in the Catalog UI.
    tags : List[str], optional
        List of tags for the node (default is None)
    dependencies : List[Node], optional
        List of other dependent nodes
    output : Schema, optional
        Defines the ouput schema of the node (default is None)
    spark_context : bool, optional
        True, returns spark context as argument to the funtion (default is False)


    .. highlight:: python
    .. code-block:: python

        # Syntax
        @node(
            type="pyspark" or "pandas_on_spark" or "pandas",
            description="this is a description of what this node does",
            tags=["a", "list", "of", "tags"],
            dependencies=[other_node_1, other_node_2, ...],
            output=Schema(
                Column("col_name", String(), "a description of the column"),
            ),
            spark_context = True or False
        )
        def your_function_name(other_node_1, other_node_2, ...):
            # YOUR TRANSFORMATION LOGIC HERE
            # use pandas syntax if type is `pandas` or `pandas_on_spark`
            # use PySpark syntax if type is `pyspark`
            return dataframe


    .. highlight:: python
    .. code-block:: python

        # Node without dependency
        from flypipe.node import node
        from flypipe.schema import Schema, Column
        from flypipe.schema.types import String
        import pandas as pd
        @node(
            type="pandas",
            description="Only outputs a pandas dataframe",
            dependencies = [
                t0.select("fruit").alias("df")
            ],
            output=Schema(
                t0.output.get("fruit"),
                Column("flavour", String(), "fruit flavour")
            )
        )
        def t1(df):
            categories = {'mango': 'sweet', 'lemon': 'citric'}
            df['flavour'] = df['fruit']
            df = df.replace({'flavour': categories})
            return df


    .. highlight:: python
    .. code-block:: python

        # Node with dependency
        from flypipe.node import node
        from flypipe.schema import Schema, Column
        from flypipe.schema.types import String
        import pandas as pd
        @node(
            type="pandas",
            description="Only outputs a pandas dataframe",
            dependencies = [
                t0.select("fruit").alias("df")
            ],
            output=Schema(
                t0.output.get("fruit"),
                Column("flavour", String(), "fruit flavour")
            )
        )
        def t1(df):
            categories = {'mango': 'sweet', 'lemon': 'citric'}
            df['flavour'] = df['fruit']
            df = df.replace({'flavour': categories})
            return df
    """

    def decorator(func):
        kwargs["type"] = type
        return Node(func, *args, **kwargs)

    return decorator
