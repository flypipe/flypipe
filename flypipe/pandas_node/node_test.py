import pandas as pd
import pytest
from flypipe.pandas_node.node import PandasNode
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from flypipe.schema.type import SchemaType


TEST_SCHEMA = Schema([
    Column('name', SchemaType.STRING), Column('age', SchemaType.INTEGER), Column('gender', SchemaType.STRING),
    Column('is_registered', SchemaType.BOOLEAN)
])

class TestPandasNode:

    def test_convert_dataframe_invalid_type(self):
        df = pd.DataFrame({'name': ['Albert', 'Bob']})
        with pytest.raises(ValueError) as ex:
            PandasNode.convert_dataframe(df, 'nonsense')
        assert str(ex.value) == 'No mapping defined to convert a pandas node result to type "nonsense"'

    def test_validate_dataframe_missing_columns(self):
        """If we have missing columns in the input dataframe the validation should throw an appropriate error"""
        df = pd.DataFrame({'name': ['Albert', 'Bob', 'Christina'], 'gender': ['male', 'male', 'female']})
        with pytest.raises(TypeError) as ex:
            PandasNode.validate_dataframe(TEST_SCHEMA, df)
        assert str(ex.value) == '- Column "Column(name=\'age\', type=<SchemaType.INTEGER: 3>)" missing from dataframe\n' \
            '- Column "Column(name=\'is_registered\', type=<SchemaType.BOOLEAN: 4>)" missing from dataframe'

    def test_validate_dataframe_extra_columns(self):
        """If we have extra columns in the input dataframe then the validation should strip them from the output"""
        df = pd.DataFrame({'name': ['Albert', 'Bob', 'Christina'], 'age': [30, 30, 30], 'gender': ['male', 'male', 'female'], 'is_registered': [False, True, True], 'sneaky_column': [1,1,1]})
        df = PandasNode.validate_dataframe(TEST_SCHEMA, df)
        assert df.columns.tolist() == ['name', 'age', 'gender', 'is_registered']

    def test_validate_dataframe_type_mismatch(self):
        """
        Any type mismatch between the flypipe schema representation of the pandas schema and the requested schema
        should trigger an error
        """
        df = pd.DataFrame({'name': ['Albert', 'Bob', 'Christina'], 'age': [30, 30, 30],
                           'gender': [1, 1, 1], 'is_registered': ['a', 'b', 'c']})
        with pytest.raises(TypeError) as ex:
            df = PandasNode.validate_dataframe(TEST_SCHEMA, df)
        assert str(ex.value) == '- Column gender is of pandas type "int64" but we are expecting type "<class ' \
             '\'object\'>"\n' \
             '- Column is_registered is of pandas type "object" but we are expecting type ' \
             '"<class \'bool\'>"'
