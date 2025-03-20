from flypipe import node
from flypipe.schema import Schema, Column
from flypipe.schema.types import String, Boolean


class TestSchema:
    def test_schema_are_equal(self):
        schema1 = Schema(Column("col1", String(), "desc"))
        schema2 = Schema(Column("col1", String(), "desc"))

        assert schema1 == schema2

    def test_schema_are_equal_column_out_of_order(self):
        schema1 = Schema(
            Column("col1", String(), "desc"), Column("col2", String(), "desc")
        )
        schema2 = Schema(
            Column("col2", String(), "desc"), Column("col1", String(), "desc")
        )

        assert schema1 == schema2

    def test_schema_are_equal_same_attribute(self):
        schema1 = Schema(Column("col1", String(), "desc", pk=True))
        schema2 = Schema(Column("col1", String(), "desc", pk=True))

        assert schema1 == schema2

    def test_schema_are_not_equal_different_attribute(self):
        schema1 = Schema(Column("col1", String(), "desc", pk=True))
        schema2 = Schema(Column("col1", String(), "desc"))

        assert schema1 != schema2

    def test_schema_are_equal_same_relationships(self):
        @node(type="pandas", output=Schema(Column("col1", String(), "desc")))
        def f():
            pass

        schema1 = Schema(Column("col1", String(), "desc").many_to_one(f.output.col1))
        schema2 = Schema(Column("col1", String(), "desc").many_to_one(f.output.col1))
        assert schema1 == schema2

    def test_schema_are_different_with_different_relationships(self):
        @node(type="pandas", output=Schema(Column("col1", String(), "desc")))
        def f():
            pass

        schema1 = Schema(Column("col1", String(), "desc").many_to_one(f.output.col1))
        schema2 = Schema(Column("col1", String(), "desc").one_to_many(f.output.col1))
        assert schema1 != schema2

        schema1 = Schema(Column("col1", String(), "desc").many_to_one(f.output.col1))
        schema2 = Schema(
            Column("col1", String(), "desc").many_to_one(f.output.col1, "has")
        )
        assert schema1 != schema2

        schema1 = Schema(Column("col1", String(), "desc").many_to_one(f.output.col1))
        schema2 = Schema(Column("col1", String(), "desc"))
        assert schema1 != schema2

    def test_schema_are_differnt_with_different_type(self):
        schema1 = Schema(Column("col1", String(), "desc"))
        schema2 = Schema(Column("col1", Boolean(), "desc"))

        assert schema1 != schema2

    def test_schema_are_different_with_different_description(self):
        schema1 = Schema(Column("col1", String(), "desc"))
        schema2 = Schema(Column("col1", String()))

        assert schema1 != schema2
