import pytest
from flypipe.node_run_data import NodeRunData
from flypipe.schema import Schema, Column
from flypipe.schema.types import Unknown, Integer, String


class TestNodeRunData:

    def test_output_columns_and_schema_specific_columns(self):
        """
        If we request specific columns then we expect the output columns and schema to be the superset of these.
        Without a proper schema, the returned schema should have unknown types.
        """
        node_context = NodeRunData()
        node_context.add_output_columns(['c1', 'c2'])
        node_context.add_output_columns(['c2', 'c3'])
        assert node_context.output_columns == ['c1', 'c2', 'c3']
        assert [column.name for column in node_context.output_schema.columns] == ['c1', 'c2', 'c3']
        assert all([isinstance(column.type, Unknown) for column in node_context.output_schema.columns])

    def test_output_columns_and_schema_specific_columns_with_schema(self):
        """
        If we request specific columns then we expect the output columns and schema to be the superset of these.
        The returned schema should inherit types from the schema the NodeRunData was built with.
        """
        node_context = NodeRunData(Schema([Column('c1', Integer()), Column('c2', String()), Column('c3', Integer())]))
        node_context.add_output_columns(['c1', 'c2'])
        node_context.add_output_columns(['c2'])
        assert node_context.output_columns == ['c1', 'c2']
        assert [column.name for column in node_context.output_schema.columns] == ['c1', 'c2']
        assert isinstance(node_context.output_schema.columns[0].type, Integer)
        assert isinstance(node_context.output_schema.columns[1].type, String)

    def test_output_columns_and_schema_all_columns_no_schema(self):
        """
        If we request all columns and have no schema then we expect the output columns and schema to be None, to
        indicate that we have no knowledge of the shape of the output.
        """
        node_context = NodeRunData()
        node_context.add_output_columns(['c1', 'c3'])
        node_context.add_output_columns(None)
        assert node_context.output_columns is None
        assert node_context.output_schema is None

    def test_output_columns_and_schema_all_columns_with_schema(self):
        """
        If we request all columns and have a schema then we expect the output columns and schema to be the same as the
        schema the NodeRunData was built from.
        """
        node_context = NodeRunData(Schema([Column('c1', String()), Column('c2', String())]))
        node_context.add_output_columns(['c1'])
        node_context.add_output_columns(None)
        assert node_context.output_columns == ['c1', 'c2']
        assert [column.name for column in node_context.output_schema.columns] == ['c1', 'c2']
        assert isinstance(node_context.output_schema.columns[0].type, String)
        assert isinstance(node_context.output_schema.columns[1].type, String)

    def test_output_columns_and_schema_request_invalid_column(self):
        """
        If we request a column that doesn't exist on the schema then an appropriate error should be raised.
        """
        node_context = NodeRunData(Schema([Column('c1', String())]))
        with pytest.raises(ValueError) as ex:
            node_context.add_output_columns(['c1', 'c2'])
        assert str(ex.value) == 'Requested columns {\'c2\'} that are not present on the node'
