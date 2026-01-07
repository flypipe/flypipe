import re
import sys
from typing import List, Union

from pyspark.sql import SparkSession

from flypipe.cache.cache import Cache
from flypipe.dependency.preprocess_mode import PreprocessMode
from flypipe.config import get_config
from flypipe.dependency.node_input import InputNode
from flypipe.node_dependencies_mixin import NodeDependenciesMixin
from flypipe.node_type import NodeType
from flypipe.run_context import RunContext
from flypipe.schema import Schema
from flypipe.utils import DataFrameType, get_logger
from flypipe.runner import Runner

logger = get_logger()


class Node(NodeDependenciesMixin):
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

        self.spark_context = spark_context
        self.requested_columns = requested_columns
        self.node_graph = None

        if cache is not None and not isinstance(cache, Cache):
            raise TypeError("cache is not of type flypipe.cache.Cache")

        self.cache = cache

        # FWe declare that this node as parent of the cache
        if self.cache is not None:
            self.cache.set_parent(self)

        self.output_schema = output

        # For each column if the schema, declare that this node is the parent for all of them
        # this for loop leaves columns aware of its owner to guide relationships definition
        if self.output_schema is not None:
            self.output_schema.set_parent(self)

    @property
    def output(self):
        schema = self.output_schema.copy()
        schema.reset(relationships=True, pk=True)
        return schema

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

    def __call__(self, *args):
        return self.function(*args)

    def run(
        self,
        spark: SparkSession = None,
        max_workers: int = 1,
        inputs: dict = None,
        pandas_on_spark_use_pandas: bool = False,
        parameters: dict = None,
        cache: dict = None,
        preprocess: Union[dict, PreprocessMode] = None,
        debug: bool = False,
    ):
        """
        Execute the node and its upstream dependencies, returning the final result.

        This method orchestrates the execution of the entire transformation graph from the
        target node (self) backwards through its dependencies. It supports both sequential
        and parallel execution, caching, CDC (Change Data Capture), and various preprocessing modes.

        Parameters
        ----------
        spark : SparkSession, optional
            The Spark session to use for Spark-based transformations. Required for nodes
            that work with Spark DataFrames or execute Spark SQL queries (default: None).
        max_workers : int, optional
            Maximum number of parallel workers for concurrent node execution. If None,
            defaults to 1 (sequential execution). When > 1, independent nodes in the graph
            will be executed in parallel using ThreadPoolExecutor. The actual parallelism
            is capped at `os.cpu_count() - 1` (default: 1).
        inputs : dict, optional
            Dictionary mapping Node objects to their pre-computed DataFrames or input parameters.
            Nodes provided here will skip execution and use the supplied data instead. This is
            useful for:
            - Providing external data sources
            - Testing specific subgraphs
            - Incremental updates in CDC workflows
            Format: {Node: DataFrame} or {Node: {"param1": value1, ...}} (default: None).
        pandas_on_spark_use_pandas : bool, optional
            When True, converts pandas-on-Spark DataFrames to regular pandas DataFrames.
            This can improve performance for small datasets that fit in memory (default: False).
        parameters : dict, optional
            Dictionary mapping Node objects to their runtime parameters. These parameters
            are passed to the node's transformation function, allowing dynamic behavior.
            Format: {Node: {"param1": value1, "param2": value2, ...}} (default: None).
        cache : dict, optional
            Dictionary mapping Node objects to their CacheMode. Controls caching behavior
            for specific nodes in the execution graph. Supported modes:
            - CacheMode.MERGE: Incremental caching with CDC support
            - CacheMode.OVERWRITE: Replace existing cache
            - CacheMode.DISABLE: Skip caching for this run
            Format: {Node: CacheMode} (default: None).
        preprocess : Union[dict, PreprocessMode], optional
            Controls preprocessing of upstream dependencies. Can be:
            - PreprocessMode enum: Applied to all dependencies globally
            - dict: Mapping {ParentNode: {DependencyNode: PreprocessMode}} for fine-grained control
            Preprocessing includes column selection, type conversion, and format alignment
            (default: None, which means PreprocessMode.ACTIVE).
        debug : bool, optional
            When True, enables debug logging in the Runner. Instead of using print statements,
            the Runner will use logger.debug() for all execution logs, which can be controlled
            via Python's logging configuration (default: False).

        Returns
        -------
        DataFrame
            The computed DataFrame from the target node, converted to the node's
            configured dataframe_type (pandas, PySpark, or pandas-on-Spark).

        Examples
        --------
        Basic execution:

        >>> result = my_node.run(spark)

        Parallel execution with 4 workers:

        >>> result = my_node.run(spark, max_workers=4)

        Providing input data and parameters:

        >>> result = my_node.run(
        ...     spark,
        ...     inputs={source_node: external_df},
        ...     parameters={transform_node: {"threshold": 0.5}}
        ... )

        Incremental CDC execution:

        >>> result = my_node.run(
        ...     spark,
        ...     cache={node_a: CacheMode.MERGE, node_b: CacheMode.MERGE},
        ...     inputs={source_node: new_rows_df}
        ... )

        Notes
        -----
        - The execution uses Runner, which builds a logical execution plan and executes
          nodes level-by-level based on dependencies.
        - Cached nodes with CacheMode.MERGE trigger CDC filtering for upstream dependencies,
          processing only changed data.
        - All intermediate results are stored in run_context.node_results for memoization.
        - The method is thread-safe for parallel execution when max_workers > 1.
        """
        inputs = inputs or {}

        run_context = RunContext(
            spark=spark,
            max_workers=max_workers,
            provided_inputs=inputs,
            pandas_on_spark_use_pandas=pandas_on_spark_use_pandas,
            parameters=parameters,
            cache_modes=cache,
            dependencies_preprocess_modes=preprocess,
            debug=debug,
        )

        self.create_graph(run_context)
        execution_graph = self.node_graph.get_execution_graph(run_context)

        runner = Runner(node_graph=execution_graph, run_context=run_context)

        runner.run(target_node=self)

        end_node_name = self.node_graph.get_end_node_name(self.node_graph.graph)
        end_node = self.node_graph.get_transformation(end_node_name)
        return run_context.get_graph_result(end_node)

    @property
    def dataframe_type(self):
        return self.DATAFRAME_TYPE_MAP[self.type]

    def html(
        self,
        spark=None,
        height=700,
        inputs=None,
        pandas_on_spark_use_pandas=False,
        parameters=None,
        cache=None,
        preprocess: Union[dict, PreprocessMode] = None,
        debug: bool = False,
    ):
        """
        Retrieves html string of the graph to be executed.

        Parameters
        ----------
        spark : SparkSession, optional
            The Spark session to use for Spark-based transformations. Required for nodes
            that work with Spark DataFrames or execute Spark SQL queries (default: None).
        height : int, default 700
            viewport height in pixels
        inputs : dict, default None
            dictionary where keys are Nodes and values dataframes, these dataframes will skip the nodes executions as
            they have been provided
        pandas_on_spark_use_pandas : bool, default False
            If True, convert and runs `pandas_on_spark` as `pandas`
        parameters : dict, default None
            dictionary dict(Node,dict(str,obj)) of parameters to be given to the nodes when executing them.
        cache : dict, optional
            Dictionary mapping Node objects to their CacheMode. Controls caching behavior
            for specific nodes in the execution graph. Supported modes:
            - CacheMode.MERGE: Incremental caching with CDC support
            - CacheMode.OVERWRITE: Replace existing cache
            - CacheMode.DISABLE: Skip caching for this run
            Format: {Node: CacheMode} (default: None).
        preprocess : Union[dict, PreprocessMode], optional
            Controls preprocessing of upstream dependencies. Can be:
            - PreprocessMode enum: Applied to all dependencies globally
            - dict: Mapping {ParentNode: {DependencyNode: PreprocessMode}} for fine-grained control
            Preprocessing includes column selection, type conversion, and format alignment
            (default: None, which means PreprocessMode.ACTIVE).
        debug : bool, optional
            When True, enables debug logging in the Runner. Instead of using print statements,
            the Runner will use logger.debug() for all execution logs, which can be controlled
            via Python's logging configuration (default: False).

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
    """Nodes are the fundamental building block of Flypipe. Simply apply the node function as a decorator to a
    transformation function in order to declare the transformation as a Flypipe node.

    Parameters:
        type (str): Type of the node transformation "pandas", "pandas_on_spark", "pyspark", "spark_sql"
        description (str,optional): Description of the node. Defaults to `None`.
        group (str,optional): Group the node falls under, nodes in the same group are clustered together in the Catalog UI. Defaults to `None`.
        tags (List[str],optional): List of tags for the node. Defaults to `None`.
        dependencies (List[Node],optional): List of other dependent nodes. Defaults to `None`.
        output (Schema,optional): Defines the output schema of the node. Defaults to `None`.
        spark_context (bool,optional): True, returns spark context as argument to the function. Defaults to `False`.

    Examples

    ``` py
    # Syntax
    @node(
        type="pyspark", "pandas_on_spark" or "pandas",
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
    ```

    ``` py
    # Node without dependency
    from flypipe.node import node
    from flypipe.schema import Schema, Column
    from flypipe.schema.types import String
    import pandas as pd
    @node(
        type="pandas",
        description="Only outputs a pandas dataframe",
        output=Schema(
            t0.output.get("fruit"),
            Column("flavour", String(), "fruit flavour")
        )
    )
    def t1(df):
        return pd.DataFrame({"fruit": ["mango"], "flavour": ["sweet"]})
    ```

    ``` py
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
    ```

    """

    def decorator(func):
        kwargs["type"] = type
        return Node(func, *args, **kwargs)

    return decorator
