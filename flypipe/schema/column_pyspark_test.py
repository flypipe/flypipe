import os
import pytest

from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") not in ["SPARK", "SPARK_CONNECT"],
    reason="PySpark tests require RUN_MODE=SPARK or SPARK_CONNECT",
)
class TestColumnPySpark:
    """Tests for column - PySpark"""

    def test_relationship_is_valid_on_output(self):
        def replace_chars(s: str) -> str:
            old_chars = [" ", "\n", "\t"]
            new_chars = ["", "", ""]
            translation_table = str.maketrans(dict(zip(old_chars, new_chars)))
            return s.translate(translation_table)

        @node(type="pyspark", output=Schema(Column("node_1_id", String(), "node_1 id")))
        def node_1():
            return None

        @node(
            type="pyspark",
            output=Schema(
                Column("node_2_id", String(), "node_2 id").many_to_one(
                    node_1.output.node_1_id, "as of"
                )
            ),
        )
        def node_2():
            return None

        @node(
            type="pyspark",
            output=Schema(
                node_2.output.node_2_id.many_to_one(node_2.output.node_2_id, "relates")
            ),
        )
        def node_3():
            return None

        col = node_2.output_schema.node_2_id
        col_expected = """
        Parent: node_2
        Column: node_2_id
        Data Type: String()
        Description: 'node_2 id'
        Foreign Keys:
                node_2.node_2_id N:1 (as of) node_1.node_1_id
        PK: False"""
        assert replace_chars(str(col)) == replace_chars(col_expected)

        col = node_3.output_schema.node_2_id
        col_expected = """
                Parent: node_3
                Column: node_2_id
                Data Type: String()
                Description: 'node_2 id'
                Foreign Keys:
                        node_3.node_2_id N:1 (relates) node_2.node_2_id
                PK: False"""
        assert replace_chars(str(col)) == replace_chars(col_expected)

