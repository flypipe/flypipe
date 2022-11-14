from flypipe.output_column_set import OutputColumnSet, OutputColumnStatus
from flypipe.schema import Schema, Column
from flypipe.schema.types import Unknown


class TestOutputColumnSet:

    def test_status_and_columns_specific_columns(self):
        output_columns = OutputColumnSet()
        output_columns.add_columns(['c1', 'c2'])
        output_columns.add_columns(['c3'])
        assert output_columns._status==OutputColumnStatus.SOME_COLUMNS
        assert output_columns.columns == ['c1', 'c2', 'c3']

    def test_status_and_columns_specific_columns_with_schema(self):
        output_columns = OutputColumnSet(Schema([Column('c1', Unknown()), Column('c2', Unknown())]))
        output_columns.add_columns(['c1', 'c3'])
        output_columns.add_columns(['c2'])
        assert output_columns._status==OutputColumnStatus.SOME_COLUMNS
        assert output_columns.columns==['c1', 'c2']

    def test_status_columns_all_columns_no_schema(self):
        output_columns = OutputColumnSet()
        output_columns.add_columns(['c1', 'c3'])
        output_columns.add_columns(None)
        assert output_columns._status==OutputColumnStatus.ALL_COLUMNS
        assert output_columns.columns == []

    def test_status_columns_all_columns_with_schema(self):
        output_columns = OutputColumnSet(Schema([Column('c1', Unknown()), Column('c2', Unknown())]))
        output_columns.add_columns(['c1', 'c3'])
        output_columns.add_columns(None)
        assert output_columns._status==OutputColumnStatus.ALL_COLUMNS
        assert output_columns.columns == ['c1', 'c2']
