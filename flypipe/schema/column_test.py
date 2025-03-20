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

        relationships = t2.output.t2c1.relationships
        assert relationships[t1.output.t1c1].type == RelationshipType.ONE_TO_ONE
        assert relationships[t1.output.t1c1].description == "my desc"

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

        relationships = t2.output.t2c1.relationships
        assert relationships[t1.output.t1c1].type == RelationshipType.MANY_TO_ONE
        assert relationships[t1.output.t1c1].description == "my desc"

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

        relationships = t2.output.t2c1.relationships
        assert relationships[t1.output.t1c1].type == RelationshipType.ONE_TO_MANY
        assert relationships[t1.output.t1c1].description == "my desc"

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

        relationships = t2.output.t2c1.relationships
        assert relationships[t1.output.t1c1].type == RelationshipType.MANY_TO_MANY
        assert relationships[t1.output.t1c1].description == "my desc"

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
                        .one_to_many(t1.output.t1c1, "my desc")
                        .many_to_many(t1.output.t1c1, "my desc"),
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
            == """Column:t1c1Parent:NoneDataType:String()Description:''"""
        )
