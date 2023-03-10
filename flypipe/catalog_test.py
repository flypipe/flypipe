import inspect
from pathlib import Path
import pandas as pd
from flypipe.catalog import Catalog
from flypipe import node, node_function
from flypipe.config import config_context
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
    dependencies=[t1.select("c2", "c3").alias("df")],
    description="Description for t2",
    tags=["train", "test"],
    output=Schema([Column("c2", String(), "c2 desc")]),
)
def t2(df):
    return df["c2"]


@node(
    type="pandas",
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
                for node_def in catalog.get_node_defs()
            ]
        ) == [
            (
                "t1",
                [
                    "flypipe_catalog_test_function_t2_t2",
                    "flypipe_catalog_test_function_t3_t3",
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
            [
                (node_def["name"], node_def["successors"])
                for node_def in catalog.get_node_defs()
            ]
        ) == [("t1", ["flypipe_catalog_test_function_t2_t2"]), ("t2", [])]

    def test_get_node_defs(self):
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        assert catalog.get_node_defs() == [
            {
                "description": "Description for t2",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t2",
                "nodeKey": "flypipe_catalog_test_function_t2_t2",
                "nodeType": "pandas",
                "name": "t2",
                "predecessors": ["flypipe_catalog_test_function_t1_t1"],
                "predecessorColumns": {
                    "flypipe_catalog_test_function_t1_t1": ["c2", "c3"]
                },
                "output": [
                    {"column": "c2", "type": "String", "description": "c2 desc"},
                ],
                "successors": [],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                    {"id": "train", "text": "train"},
                    {"id": "test", "text": "test"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t2.function)),
                "isActive": True,
            },
            {
                "description": "Description for t1",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t1",
                "nodeKey": "flypipe_catalog_test_function_t1_t1",
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
                    "flypipe_catalog_test_function_t2_t2",
                    "flypipe_catalog_test_function_t3_t3",
                ],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                    {"id": "train", "text": "train"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t1.function)),
                "isActive": True,
            },
            {
                "description": "",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t3",
                "nodeKey": "flypipe_catalog_test_function_t3_t3",
                "nodeType": "pandas",
                "name": "t3",
                "predecessors": ["flypipe_catalog_test_function_t1_t1"],
                "predecessorColumns": {"flypipe_catalog_test_function_t1_t1": []},
                "output": [],
                "successors": [],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                    {"id": "misc", "text": "misc"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t3.function)),
                "isActive": True,
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

    def test_register_node_function(self):
        """
        Node functions should be expanded and the returned nodes used as per normal.
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

        catalog = Catalog()
        catalog.register_node(t4)
        assert catalog.get_node_defs() == [
            {
                "description": "",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t4",
                "nodeKey": "flypipe_catalog_test_function_t4_TestCatalog_test_register_node_function__locals__t4",
                "nodeType": "pandas",
                "name": "t4",
                "predecessors": [
                    "flypipe_catalog_test_function_t3_TestCatalog_test_register_node_function__locals__get_nodes__"
                    "locals__t3"
                ],
                "predecessorColumns": {
                    "flypipe_catalog_test_function_t3_TestCatalog_test_register_node_function__locals__get_nodes__"
                    "locals__t3": []
                },
                "output": [],
                "successors": [],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t4.function)),
                "isActive": True,
            },
            {
                "description": "",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t3",
                "nodeKey": "flypipe_catalog_test_function_t3_TestCatalog_test_register_node_function__locals__get_"
                "nodes__locals__t3",
                "nodeType": "pandas",
                "name": "t3",
                "predecessors": [
                    "flypipe_catalog_test_function_t2_TestCatalog_test_register_node_function__locals__get_nodes__"
                    "locals__t2"
                ],
                "predecessorColumns": {
                    "flypipe_catalog_test_function_t2_TestCatalog_test_register_node_function__locals__get_nodes__"
                    "locals__t2": []
                },
                "output": [],
                "successors": [
                    "flypipe_catalog_test_function_t4_TestCatalog_test_register_node_function__locals__t4"
                ],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t3.function)),
                "isActive": True,
            },
            {
                "description": "",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t2",
                "nodeKey": "flypipe_catalog_test_function_t2_TestCatalog_test_register_node_function__locals__get_"
                "nodes__locals__t2",
                "nodeType": "pandas",
                "name": "t2",
                "predecessors": [
                    "flypipe_catalog_test_function_t1_TestCatalog_test_register_node_function__locals__t1"
                ],
                "predecessorColumns": {
                    "flypipe_catalog_test_function_t1_TestCatalog_test_register_node_function__locals__t1": []
                },
                "output": [],
                "successors": [
                    "flypipe_catalog_test_function_t3_TestCatalog_test_register_node_function__locals__get_nodes__"
                    "locals__t3"
                ],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t2.function)),
                "isActive": True,
            },
            {
                "description": "",
                "filePath": str(Path("flypipe/catalog_test.py")),
                "importCmd": "from flypipe.catalog_test import t1",
                "nodeKey": "flypipe_catalog_test_function_t1_TestCatalog_test_register_node_function__locals__t1",
                "nodeType": "pandas",
                "name": "t1",
                "predecessors": [],
                "predecessorColumns": {},
                "output": [],
                "successors": [
                    "flypipe_catalog_test_function_t2_TestCatalog_test_register_node_function__locals__get_nodes__"
                    "locals__t2"
                ],
                "tags": [
                    {"id": "pandas", "text": "pandas"},
                    {"id": "Transformation", "text": "Transformation"},
                ],
                "sourceCode": inspect.getsource(inspect.getmodule(t1.function)),
                "isActive": True,
            },
        ]

    def test_get_tag_suggestions(self):
        catalog = Catalog()
        catalog.register_node(t2)
        catalog.register_node(t3)
        assert catalog.get_tag_suggestions() == [
            {"id": "Transformation", "text": "Transformation"},
            {"id": "misc", "text": "misc"},
            {"id": "pandas", "text": "pandas"},
            {"id": "test", "text": "test"},
            {"id": "train", "text": "train"},
        ]
