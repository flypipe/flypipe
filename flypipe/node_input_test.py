import pandas as pd

from flypipe import node
from flypipe.datasource.spark import Spark


@node(
    type="pandas",
)
def transformation():
    return pd.DataFrame({"c1": [1, 2], "c2": ["a", "b"], "c3": [True, False]})


class TestInputNode:
    """Tests for InputNode"""

    def test_name(self):
        node_input = transformation.select(["c1", "c2"])
        assert node_input.__name__ == transformation.__name__

    def test_key(self):
        node_input = transformation.select(["c1", "c2"])
        assert node_input.key == transformation.key

    def test_alias_unmodified_1(self):
        node_input = transformation.select(["c1", "c2"])
        assert node_input.get_alias() == "transformation"

    def test_alias_unmodified_2(self):
        node_input = Spark("schema_a.table_b").select("c1", "c2")
        assert node_input.get_alias() == "schema_a_table_b"

    def test_alias_modified(self):
        node_input = transformation.select(["c1", "c2"]).alias("customised")
        assert node_input.get_alias() == "customised"
