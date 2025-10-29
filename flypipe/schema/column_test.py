import pytest

from flypipe import node
from flypipe.config import config_context
from flypipe.schema import Schema, Column
from flypipe.schema.column import RelationshipType
from flypipe.schema.types import String


class TestColumn:
    """Tests for column"""

    def test_schema_mandatory_description(self):
        with pytest.raises(ValueError) as ex, config_context(
            require_schema_description=True
        ):

            @node(
                type="pandas",
                output=Schema(
                    [
                        Column("c1", String(), "test"),
                        Column("c2", String()),
                    ]
                ),
            )
            def transformation():
                return

        assert str(ex.value) == (
            "Descriptions on schema columns configured as mandatory but no description provided for column c2"
        )

    def test_relationship_one_to_one(self):
        print()

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").one_to_one(
                        t1.output.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert relationships[t1.output_schema.t1c1].type == RelationshipType.ONE_TO_ONE
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_relationship_many_to_one(self):
        print()

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").many_to_one(
                        t1.output.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert relationships[t1.output_schema.t1c1].type == RelationshipType.MANY_TO_ONE
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_relationship_one_to_many(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").one_to_many(
                        t1.output.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert relationships[t1.output_schema.t1c1].type == RelationshipType.ONE_TO_MANY
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_relationship_many_to_many(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t2c1", String(), "test").many_to_many(
                        t1.output.t1c1, "my desc"
                    ),
                ]
            ),
        )
        def t2():
            return

        relationships = t2.output_schema.t2c1.relationships
        assert (
            relationships[t1.output_schema.t1c1].type == RelationshipType.MANY_TO_MANY
        )
        assert relationships[t1.output_schema.t1c1].description == "my desc"

    def test_declared_more_than_one_relationship_same_column_raises_exception(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test"),
                    Column("t1c2", String()),
                ]
            ),
        )
        def t1():
            return

        with pytest.raises(ValueError):

            @node(
                type="pandas",
                output=Schema(
                    [
                        Column("t2c1", String(), "test")
                        .one_to_many(t1.output_schema.t1c1, "my desc")
                        .many_to_many(t1.output_schema.t1c1, "my desc"),
                    ]
                ),
            )
            def t2():
                return

    def test_add_extra_arguments_exists_in_column(self):

        col = Column("t1c1", String(), "test", pk=True)
        assert hasattr(col, "pk") and col.pk

    def test_print_column_without_parent(self):
        col = Column("t1c1", String())
        assert (
            str(col).replace("\n", "").replace(" ", "")
            == """Parent:NoneColumn:t1c1DataType:String()Description:''PK:False"""
        )

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

    def test_output_colum_erase_pk_and_relationships(self):
        @node(
            type="pandas",
            output=Schema(
                [
                    Column("t1c1", String(), "test", pk=True),
                ]
            ),
        )
        def t1():
            pass

        @node(
            type="pandas",
            dependencies=[t1.select("t1c1")],
            output=Schema(
                [
                    t1.output.t1c1.many_to_one(t1.output.t1c1, "my desc"),
                ]
            ),
        )
        def t2():
            pass

        @node(
            type="pandas",
            dependencies=[t2.select("t1c1")],
            output=Schema(
                [
                    t2.output.t1c1,
                ]
            ),
        )
        def t3():
            pass

        assert not t2.output.t1c1.pk
        assert not t3.output.t1c1.pk
        assert t2.output_schema.get("t1c1").relationships
        assert not t2.output.t1c1.relationships
        assert not t3.output_schema.get("t1c1").relationships
        assert isinstance(t3.output.t1c1.set_pk(True), Column)
        assert t3.output.t1c1.set_pk(True).pk
