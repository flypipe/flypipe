import os
import pytest

from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestColumnSnowpark:
    """Tests for column - Snowpark"""

    def test_relationship_is_valid_on_output(self):
        def replace_chars(s: str) -> str:
            old_chars = [" ", "\n", "\t"]
            new_chars = ["", "", ""]
            translation_table = str.maketrans(dict(zip(old_chars, new_chars)))
            return s.translate(translation_table)

        @node(type="snowpark", output=Schema(Column("NODE_1_ID", String(), "node_1 id")))
        def node_1():
            return None

        @node(
            type="snowpark",
            output=Schema(
                Column("NODE_2_ID", String(), "node_2 id").many_to_one(
                    node_1.output.NODE_1_ID, "as of"
                )
            ),
        )
        def node_2():
            return None

        @node(
            type="snowpark",
            output=Schema(
                node_2.output.NODE_2_ID.many_to_one(node_2.output.NODE_2_ID, "relates")
            ),
        )
        def node_3():
            return None

        col = node_2.output_schema.NODE_2_ID
        col_expected = """
        Parent: node_2
        Column: NODE_2_ID
        Data Type: String()
        Description: 'node_2 id'
        Foreign Keys:
                node_2.NODE_2_ID N:1 (as of) node_1.NODE_1_ID
        PK: False"""
        assert replace_chars(str(col)) == replace_chars(col_expected)

        col = node_3.output_schema.NODE_2_ID
        col_expected = """
                Parent: node_3
                Column: NODE_2_ID
                Data Type: String()
                Description: 'node_2 id'
                Foreign Keys:
                        node_3.NODE_2_ID N:1 (relates) node_2.NODE_2_ID
                PK: False"""
        assert replace_chars(str(col)) == replace_chars(col_expected)

