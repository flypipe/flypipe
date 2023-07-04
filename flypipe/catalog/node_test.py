import pandas as pd

from flypipe import node
from flypipe.catalog.node import CatalogNode


# pylint: disable=too-few-public-methods, missing-class-docstring
class TestNode:
    def test__is_active(self):
        @node(type="pandas")
        def t0():
            return pd.DataFrame(data={"c": [1]})

        t0.run()
        catalog_node = CatalogNode(t0, t0.node_graph)
        # pylint: disable=protected-access
        assert catalog_node._get_is_active()

        t0.run(inputs={t0: pd.DataFrame(data={"c": [1]})})
        catalog_node = CatalogNode(t0, t0.node_graph)
        # pylint: disable=protected-access
        assert not catalog_node._get_is_active()
