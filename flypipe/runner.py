import logging
from datetime import datetime
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from flypipe.cache import CacheMode, CDCCache
from flypipe.run_status import RunStatus
from flypipe.run_context import RunContext
from flypipe.utils import log

logger = logging.getLogger(__name__)


class Runner:
    """
    CDC-aware graph runner with parallel execution support.

    This runner:
    1. Creates a logical execution plan with levels
    2. Executes nodes in parallel within each level
    3. A node can run when all its dependencies have been computed
    """

    def __init__(self, node_graph, run_context: RunContext):
        """
        Initialize the Runner.

        Parameters
        ----------
        node_graph : NodeGraph
            The NodeGraph object containing the execution graph
        run_context : RunContext
            The run context containing inputs, parameters, and results storage
        """
        self.node_graph = node_graph
        self.graph = node_graph.graph
        self.run_context = run_context
        self.cdc_table_exists = False

    def _log(self, message: str):
        """Log a message using logger.debug if debug mode is enabled, otherwise print."""
        if self.run_context.debug:
            log(logger, message)

    def create_execution_plan(self, target_node) -> List[List[str]]:
        """
        Create a logical execution plan with levels.

        Nodes at the same level can be executed in parallel since they don't
        depend on each other and all their dependencies are in previous levels.

        Parameters
        ----------
        target_node : Node
            The target node to execute

        Returns
        -------
        List[List[str]]
            A list of levels, where each level is a list of node keys that can
            be executed in parallel
        """

        self._log(f"📋 Creating execution plan for target node: {target_node.__name__}")
        self._log(f"{'-'*60}")

        target_key = target_node.key

        # Build the subgraph of nodes needed to compute the target
        nodes_to_compute = set()
        stack = [target_key]

        while stack:
            node_key = stack.pop()
            if node_key in nodes_to_compute:
                continue

            node_data = self.graph.nodes[node_key]
            status = node_data["status"]

            # Add this node if it needs to be computed
            if status in (RunStatus.ACTIVE, RunStatus.CACHED, RunStatus.PROVIDED_INPUT):
                nodes_to_compute.add(node_key)

                # Only traverse upstream if the node is ACTIVE (needs computation)
                if status == RunStatus.ACTIVE:
                    predecessors = list(self.graph.predecessors(node_key))
                    stack.extend(predecessors)

        # Create subgraph with only nodes to compute
        subgraph = self.graph.subgraph(nodes_to_compute)

        # Topological sort to get levels
        # Level 0: nodes with no dependencies
        # Level 1: nodes that only depend on level 0
        # etc.
        levels = []
        remaining_nodes = set(nodes_to_compute)
        computed_nodes = set()

        while remaining_nodes:
            # Find nodes whose dependencies are all computed
            current_level = []
            for node_key in remaining_nodes:
                predecessors = set(subgraph.predecessors(node_key))
                # If all predecessors are computed (or no predecessors), add to current level
                if predecessors.issubset(computed_nodes):
                    current_level.append(node_key)

            if not current_level:
                # This shouldn't happen with a valid DAG
                raise RuntimeError(
                    f"Circular dependency detected or invalid graph state. Remaining nodes: {remaining_nodes}"
                )

            levels.append(current_level)

            # Mark these nodes as computed for the next iteration
            for node_key in current_level:
                computed_nodes.add(node_key)
                remaining_nodes.remove(node_key)

        # Print the execution plan
        self._log("🗂️  Execution Plan:")
        for level_idx, level in enumerate(levels):
            node_names = [
                self.graph.nodes[nk]["transformation"].__name__ for nk in level
            ]
            self._log(
                f"  Level {level_idx} (parallelism: {self.run_context.max_workers}):"
            )
            for node_name in node_names:
                self._log(f"       {node_name}")

        return levels

    def run(self, target_node, process_merge: bool = True):
        """
        Execute the graph starting from the target node using parallel execution.

        Parameters
        ----------
        target_node : Node
            The target node to execute
        process_merge : bool, optional
            Whether to process nodes with CacheMode.MERGE (default: True)
        """
        # Get max_workers from run_context
        max_workers = self.run_context.max_workers

        self._log(f"{'='*60}")
        self._log(
            f"🚀 Runner: Starting execution for target node: {target_node.__name__} (max_workers={max_workers})"
        )
        self._log(f"{'='*60}\n")

        # Create execution plan
        execution_plan = self.create_execution_plan(target_node)

        # Execute level by level
        target_node_key = target_node.key

        for level_idx, level in enumerate(execution_plan):
            self._log(f"\n📊 Executing Level {level_idx} ({len(level)} nodes)")
            self._log(f"{'-'*60}")

            self._execute_level(level, target_node_key, process_merge, max_workers)

        self._log("\n✅  Runner: Execution completed 👍")
        self._log(f"{'='*60}")

    def _ensure_cdc_tables_exist_for_level(self, level: List[str]):
        """
        Ensure CDC tables exist for all nodes in a level.

        This prevents concurrent table creation conflicts when nodes execute in parallel.

        Parameters
        ----------
        level : List[str]
            List of node keys in the current level
        """
        if self.cdc_table_exists:
            return

        for node_key in level:
            node_data = self.graph.nodes[node_key]
            cache_context = node_data["node_run_context"].cache_context
            if cache_context and isinstance(cache_context.cache, CDCCache):
                self._log("  🔧 Ensuring CDC tables exist")
                cache_context.create_cdc_table()
                self.cdc_table_exists = True
                break

    def _execute_level(
        self,
        level: List[str],
        target_node_key: str,
        process_merge: bool,
        max_workers: int,
    ):
        """
        Execute all nodes in a level in parallel or sequentially.

        Parameters
        ----------
        level : List[str]
            List of node keys to execute
        target_node_key : str
            The key of the original target node (for CDC filtering)
        process_merge : bool
            Whether to process nodes with CacheMode.MERGE
        max_workers : int
            Maximum number of parallel workers (1 = sequential)
        """
        # Ensure CDC tables exist before execution to avoid concurrent creation conflicts
        self._ensure_cdc_tables_exist_for_level(level)

        if len(level) == 1 or max_workers == 1:
            # Single node or sequential execution, execute directly without thread pool
            execution_mode = "single node" if len(level) == 1 else "sequential"
            self._log(f"  🔹 Executing {len(level)} node(s) ({execution_mode})")
            for node_key in level:
                node_name = self.graph.nodes[node_key]["transformation"].__name__
                self._log(f"    ▶️  Executing {node_name}")
                self._process_single_node(node_key, target_node_key, process_merge)
        else:
            # Multiple nodes, execute in parallel
            self._log(
                f"  🔸 Executing {len(level)} nodes in parallel (max_workers={max_workers})"
            )

            with ThreadPoolExecutor(
                max_workers=min(max_workers, len(level))
            ) as executor:
                # Submit all nodes in this level
                future_to_node = {}
                for node_key in level:
                    node_name = self.graph.nodes[node_key]["transformation"].__name__
                    self._log(f"    ⏺️  Submitting {node_name} to executor")
                    future = executor.submit(
                        self._process_single_node,
                        node_key,
                        target_node_key,
                        process_merge,
                    )
                    future_to_node[future] = node_key

                # Wait for all nodes to complete
                for future in as_completed(future_to_node):
                    node_key = future_to_node[future]
                    node_name = self.graph.nodes[node_key]["transformation"].__name__
                    try:
                        future.result()
                        self._log(f"    ✅ {node_name} completed successfully")
                    except Exception as e:
                        self._log(f"    ❌ {node_name} failed with error: {e}")
                        raise

    def _execute_transformation(
        self,
        node_transformation,
        spark,
        requested_columns: list,
        node_run_context,
        **inputs,
    ):
        """
        Execute a node's transformation function with proper parameter handling.

        Parameters
        ----------
        node_transformation : Node
            The node to execute
        spark : SparkSession
            Spark session
        requested_columns : list
            List of requested output columns
        node_run_context : NodeRunContext
            Node-specific run context
        **inputs : dict
            Input dependencies

        Returns
        -------
        DataFrame
            Result of the transformation
        """
        parameters = inputs
        if node_transformation.spark_context:
            parameters["spark"] = spark
        if node_transformation.requested_columns:
            parameters["requested_columns"] = requested_columns

        if node_run_context.parameters:
            parameters = {**parameters, **node_run_context.parameters}

        result = node_transformation.function(**parameters)
        if node_transformation.type == "spark_sql":
            # Spark SQL functions only return the text of a SQL query, we will need to execute this command.
            if not spark:
                raise ValueError(
                    "Unable to run spark_sql type node without spark being provided in the transformation.run call"
                )
            result = spark.sql(result)

        return result

    def _process_single_node(
        self, node_key: str, target_node_key: str, process_merge: bool = True
    ):
        """
        Process a single node (non-recursive).
        All dependencies must already be computed and stored in run_context.node_results.

        Parameters
        ----------
        node_key : str
            The key of the node to execute
        target_node_key : str
            The key of the original target node (for CDC filtering)
        process_merge : bool
            Whether to process nodes with CacheMode.MERGE
        """

        # Get node metadata from graph
        node_data = self.graph.nodes[node_key]
        node_transformation = node_data["transformation"]
        status = node_data["status"]
        cache_context = node_data["node_run_context"].cache_context
        node_name = node_transformation.__name__

        result = None
        datetime_start_process_transformation = datetime.now()

        # Handle different node statuses
        if status == RunStatus.PROVIDED_INPUT:
            # Get from provided inputs
            self._log(f"     📥 {node_name}: Loading from provided input")
            return

        elif status == RunStatus.CACHED:
            # Read from cache
            self._log(f"     💾 {node_name}: Reading from cache")
            result = cache_context.read()

        elif status == RunStatus.ACTIVE:
            # Execute transformation
            self._log(f"     ▶️  {node_name}: Running transformation")

            # Check for MERGE mode
            if (
                cache_context
                and cache_context.cache_mode == CacheMode.MERGE
                and target_node_key != node_key
            ):
                if process_merge:
                    # The node is marked as cached and requested to be Merged
                    self._log(
                        f"\n\n{'*'*10} Node '{node_name}' MERGE mode - recursively processing as target node {'*'*10}"
                    )
                    self.run(
                        self.graph.nodes[node_key]["transformation"],
                        process_merge=False,
                    )
                    self._log(
                        f"{'*'*26} Finished Node '{node_name}' MERGE mode {'*'*26}\n"
                    )
                    return

            # Normal ACTIVE node - get dependencies and execute
            target_transformation = self.graph.nodes[target_node_key]["transformation"]
            self._log(f"            {node_name}: Loading node dependencies dataframes")
            dependencies = node_transformation.get_node_inputs(
                self.run_context, target_transformation
            )

            # Call the transformation function
            self._log(f"         ⚙️  {node_name}: processing transformation")
            result = self._execute_transformation(
                node_transformation,
                self.run_context.spark,
                node_data["output_columns"],
                node_data["node_run_context"],
                **dependencies,
            )

        # Store result in run_context (for all statuses except SKIP)
        self._log(f"     ✓ {node_name}: Storing result in run_context")

        self.run_context.update_node_results(
            node_key, result, schema=node_transformation.output_schema
        )

        # Write cache if needed
        if cache_context and status == RunStatus.ACTIVE:
            self._log(f"     💾 {node_name}: Writing to cache")
            cache_context.write(
                self.run_context.node_results[node_key]
                .as_type(node_transformation.dataframe_type)
                .get_df()
            )

            # Write CDC metadata
            target_transformation = self.graph.nodes[target_node_key]["transformation"]
            cached_predecessors = self.node_graph.get_first_cached_predecessors(
                node_key
            )
            cache_context.write_cdc(
                cached_predecessors,
                target_transformation,
                datetime_start_process_transformation,
            )
