Changelog
=========


<h2><a href="https://github.com/flypipe/flypipe/tree/release/5.0.0" target="_blank" rel="noopener noreferrer">release/5.0.0</a></h2>


<h3>Breaking Changes</h3>

- **Removed `parallel` argument from `node.run()`**: Use `max_workers` parameter instead to control parallel execution. Set `max_workers=1` for sequential execution or `max_workers=N` for parallel execution with N workers. You can also set `FLYPIPE_NODE_RUN_MAX_WORKERS` environment variable to configure this globally (defaults to `1`).

- **Removed `FLYPIPE_DEFAULT_RUN_MODE` config**: This config previously defined parallel or sequential execution mode. Now use `FLYPIPE_NODE_RUN_MAX_WORKERS` instead to control parallelism. Set it to `1` for sequential execution or `N` for parallel execution with N workers. This can be set globally via environment variable or passed directly to `node.run()` as the `max_workers` parameter.

- **Updated `Cache` class method signatures**: The `read()` and `write()` methods now include optional CDC (Change Data Capture) parameters:
	- `read(from_node=None, to_node=None, *args, **kwargs)`: Added `from_node` and `to_node` parameters for CDC filtering
	- `write(*args, df, upstream_nodes=None, to_node=None, datetime_started_transformation=None, **kwargs)`: Added `df`, `upstream_nodes`, `to_node`, and `datetime_started_transformation` parameters for CDC metadata tracking
	- All custom cache implementations must update their method signatures to include these parameters, even if CDC functionality is not used (parameters can be ignored)
- **Added `create_cdc_table()` method to `Cache` class**: A new optional method for caches with CDC support. The base implementation is a no-op, so existing cache implementations will continue to work, but cache classes that support CDC should override this method to create their CDC metadata tables.

<h3>Commits</h3>
* <a href="https://github.com/flypipe/flypipe/issues/209" target="_blank" rel="noopener noreferrer">209 🚀 Feature Request: Implement CDC Cache in Flypipe</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.6" target="_blank" rel="noopener noreferrer">release/4.3.6</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/207" target="_blank" rel="noopener noreferrer">207 bug when inporting a column from another node colum, it is not setting pk to false</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.5" target="_blank" rel="noopener noreferrer">release/4.3.5</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/204" target="_blank" rel="noopener noreferrer">204 Bug: Output column relationships are not reset when reused in another node</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.4" target="_blank" rel="noopener noreferrer">release/4.3.4</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/200" target="_blank" rel="noopener noreferrer">200 execute jupyter notebooks for documentation</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.3" target="_blank" rel="noopener noreferrer">release/4.3.3</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/200" target="_blank" rel="noopener noreferrer">200 execute jupyter notebooks for documentation</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.2" target="_blank" rel="noopener noreferrer">release/4.3.2</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/192" target="_blank" rel="noopener noreferrer">192 Separate deploy docs from deploy wheel</a>
* <a href="https://github.com/flypipe/flypipe/issues/190" target="_blank" rel="noopener noreferrer">190 change documentation from sphinx to mkdocs</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.1" target="_blank" rel="noopener noreferrer">release/4.3.1</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/188" target="_blank" rel="noopener noreferrer">188 documentation failing to deploy</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.3.0" target="_blank" rel="noopener noreferrer">release/4.3.0</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/186" target="_blank" rel="noopener noreferrer">186 fix changelog</a>
* <a href="https://github.com/flypipe/flypipe/issues/177" target="_blank" rel="noopener noreferrer">177 Integrate sparkleframe</a>

<h2><a href="https://github.com/flypipe/flypipe/tree/release/4.2.0" target="_blank" rel="noopener noreferrer">release/4.2.0</a></h2>

* <a href="https://github.com/flypipe/flypipe/issues/173" target="_blank" rel="noopener noreferrer">173 Apply Preprocess node inputs functions</a>
* <a href="https://github.com/flypipe/flypipe/issues/175" target="_blank" rel="noopener noreferrer">175 Fix Calculate version and changelog</a>
* <a href="https://github.com/flypipe/flypipe/issues/184" target="_blank" rel="noopener noreferrer">184 fix test</a>