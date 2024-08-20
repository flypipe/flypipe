import inspect
from pathlib import Path

import pandas as pd
import pytest

from flypipe import node, node_function
from flypipe.catalog import Catalog
from flypipe.config import config_context
from flypipe.run_context import RunContext
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Integer


@node(
    type="pandas",
    tags=["train"],
    description="Description for t1",
    output=Schema(
        [
            Column("c1", String(), "c1 desc"),
            Column("c2", String(), "c2 desc"),
            Column("c3", Integer(), "c3 desc"),
        ]
    ),
)
def t1():
    return pd.DataFrame(
        {
            "c1": ["a", "b", "a"],
            "c2": ["bla", "bla", "bla"],
            "c3": [2, 1, 3],
        }
    )


@node(
    type="pandas",
    group="Training Thing",
    dependencies=[t1.select("c2", "c3").alias("df")],
    description="Description for t2",
    tags=["train", "test"],
    output=Schema([Column("c2", String(), "c2 desc")]),
)
def t2(df):
    return df["c2"]


@node(
    type="pandas",
    group="Training Thing",
    dependencies=[t1],
    tags=["misc"],
)
def t3(t1):
    return t1


class TestCatalog:
    """Tests for Catalog"""

    def test_node_successor(self):
        """
        We have an attribute to store each node's successor, this must be computed on the fly as we don't have this
        information directly on the node object unlike the other attributes. Ensure this is working properly in this
        test.
        """
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        assert sorted(
            [
                (node_def["name"], node_def["successors"])
                for node_def in catalog.get_nodes()
            ]
        ) == [
            (
                "t1",
                [
                    "flypipe_catalog_catalog_test_function_t2_t2",
                    "flypipe_catalog_catalog_test_function_t3_t3",
                ],
            ),
            ("t2", []),
            ("t3", []),
        ]

    def test_node_successor_duplicate(self):
        """
        If we accidentally register the same node multiple times the catalog should avoid making duplicate successor
        entries.
        """
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t2)
        catalog.register_node(t1)
        assert sorted(
            [(node["name"], node["successors"]) for node in catalog.get_nodes()]
        ) == [("t1", ["flypipe_catalog_catalog_test_function_t2_t2"]), ("t2", [])]

    def test_get_nodes(self):
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        assert catalog.get_nodes() == [
            {
                "description": "Description for t2",
                "filePath": str(Path("flypipe/catalog/catalog_test.py")),
                "importCmd": "from flypipe.catalog.catalog_test import t2",
                "nodeKey": "flypipe_catalog_catalog_test_function_t2_t2",
                "nodeType": "pandas",
                "name": "t2",
                "predecessors": ["flypipe_catalog_catalog_test_function_t1_t1"],
                "predecessorColumns": {
                    "flypipe_catalog_catalog_test_function_t1_t1": ["c2", "c3"]
                },
                "output": [
                    {"column": "c2", "type": "String", "description": "c2 desc"},
                ],
                "successors": [],
                "tags": [
                    {"id": "test", "name": "test"},
                    {"id": "train", "name": "train"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t2.function)),
                "isActive": True,
                "group": "Training Thing",
                "hasCache": False,
                "cacheIsDisabled": True,
                "hasProvidedInput": False,
            },
            {
                "description": "Description for t1",
                "filePath": str(Path("flypipe/catalog/catalog_test.py")),
                "importCmd": "from flypipe.catalog.catalog_test import t1",
                "nodeKey": "flypipe_catalog_catalog_test_function_t1_t1",
                "nodeType": "pandas",
                "name": "t1",
                "predecessors": [],
                "predecessorColumns": {},
                "output": [
                    {"column": "c1", "type": "String", "description": "c1 desc"},
                    {"column": "c2", "type": "String", "description": "c2 desc"},
                    {"column": "c3", "type": "Integer", "description": "c3 desc"},
                ],
                "successors": [
                    "flypipe_catalog_catalog_test_function_t2_t2",
                    "flypipe_catalog_catalog_test_function_t3_t3",
                ],
                "tags": [{"id": "train", "name": "train"}],
                "sourceCode": inspect.getsource(inspect.getmodule(t1.function)),
                "isActive": True,
                "group": None,
                "hasCache": False,
                "cacheIsDisabled": True,
                "hasProvidedInput": False,
            },
            {
                "description": "",
                "filePath": str(Path("flypipe/catalog/catalog_test.py")),
                "importCmd": "from flypipe.catalog.catalog_test import t3",
                "nodeKey": "flypipe_catalog_catalog_test_function_t3_t3",
                "nodeType": "pandas",
                "name": "t3",
                "predecessors": ["flypipe_catalog_catalog_test_function_t1_t1"],
                "predecessorColumns": {
                    "flypipe_catalog_catalog_test_function_t1_t1": []
                },
                "output": [],
                "successors": [],
                "tags": [{"id": "misc", "name": "misc"}],
                "sourceCode": inspect.getsource(inspect.getmodule(t3.function)),
                "isActive": True,
                "group": "Training Thing",
                "hasCache": False,
                "cacheIsDisabled": True,
                "hasProvidedInput": False,
            },
        ]

    def test_get_count_box_defs(self):
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        with config_context(catalog_count_box_tags="train,test"):
            result = catalog.get_count_box_defs()
        assert result == [
            {"count": 3, "label": "nodes"},
            {"count": 2, "label": "train"},
            {"count": 1, "label": "test"},
        ]

    def test_map_node_function(self):
        """
        The Catalog should be able to handle node functions by expanding them.
        """

        @node(
            type="pandas",
        )
        def t1():
            return pd.DataFrame({"c1": [1, 2, 3]})

        @node_function(node_dependencies=[t1])
        def get_nodes():
            @node(type="pandas", dependencies=[t1])
            def t2(t1):
                return t1

            @node(type="pandas", dependencies=[t2])
            def t3(t2):
                return t2

            return [t2, t3]

        @node(type="pandas", dependencies=[get_nodes])
        def t4(t3):
            return t3

        t4.create_graph(run_context=RunContext())
        catalog = Catalog()

        end_node_name = t4.node_graph.get_end_node_name(t4.node_graph.graph)
        end_node = t4.node_graph.get_transformation(end_node_name)

        catalog._map_node(end_node, node_graph=t4.node_graph)

        assert [node["name"] for node in catalog.get_nodes()] == [
            "t4",
            "get_nodes",
            "t2",
            "t1",
        ]

    def test_get_tag_suggestions(self):
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        assert catalog.get_tag_suggestions() == [
            {"id": "misc", "name": "misc"},
            {"id": "test", "name": "test"},
            {"id": "train", "name": "train"},
        ]

    def test_get_groups(self):
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        assert catalog.get_groups() == [
            {
                "id": "training_thing",
                "name": "Training Thing",
                "nodes": [
                    "flypipe_catalog_catalog_test_function_t2_t2",
                    "flypipe_catalog_catalog_test_function_t3_t3",
                ],
            }
        ]

    def test_add_node_function_fails(self):
        @node_function()
        def t1():
            @node(type="pandas")
            def t2():
                return pd.DataFrame(data={"co1": 1})

            return t2

        catalog = Catalog()

        with pytest.raises(RuntimeError):
            catalog.add_node_to_graph(t1)

    def test_register_node_function_pass(self):
        @node_function()
        def t1():
            @node(type="pandas")
            def t2():
                return pd.DataFrame(data={"co1": 1})

            return t2

        catalog = Catalog()
        catalog.register_node(t1)

    def test_map_node_function_fails(self):
        @node_function()
        def t1():
            @node(type="pandas")
            def t2():
                return pd.DataFrame(data={"co1": 1})

            return t2

        catalog = Catalog()
        with pytest.raises(RuntimeError):
            catalog._map_node(t1)

    def test_add_node_to_graph(self):
        catalog = Catalog()
        catalog.add_node_to_graph(t2)
