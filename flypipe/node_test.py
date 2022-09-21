import pandas as pd
import pytest
from flypipe.node import node
from flypipe.schema.column import Column
from flypipe.schema.schema import Schema
from flypipe.schema.type import SchemaType
from pandas.testing import assert_frame_equal


class TestNode:
    def test_run(self):
        """
        Simple hierarchy of nodes to run- node c merges parent nodes a and b. We are expecting that a) type validation
        passes fine, b) type validation per node is optional and c) that transformation c merges a and b as expected.
        """

        @node(type="pandas")
        def a():
            return pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                }
            )

        @node(type="pandas")
        def b():
            return pd.DataFrame(
                {"name": ["Albert", "Bob", "Chris"], "age": [30, 25, 28]}
            )

        @node(
            type="pandas",
            inputs=[
                a,
                (
                    b,
                    Schema(
                        [
                            Column("name", SchemaType.STRING),
                            Column("age", SchemaType.INTEGER),
                        ]
                    ),
                ),
            ],
            output=Schema(
                [
                    Column("name", SchemaType.STRING),
                    Column("age", SchemaType.INTEGER),
                    Column("fruit", SchemaType.STRING),
                ]
            ),
        )
        def c(a, b):
            return a.merge(b, on="name")

        assert_frame_equal(
            c.run(),
            pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                    "age": [30, 25, 28, 28],
                }
            ),
        )

    def test_run_input_validation_type_mismatch(self):
        """
        The output of transformation a is used as an input of b. If the input schema specified in b doesn't match the
        types of what was passed in by a then throw an appropriate error
        """

        @node(type="pandas")
        def a():
            return pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                }
            )

        @node(
            type="pandas",
            inputs=[
                (
                    a,
                    Schema(
                        [
                            Column("name", SchemaType.INTEGER),
                            Column("fruit", SchemaType.BOOLEAN),
                        ]
                    ),
                ),
            ],
            output=Schema(
                [
                    Column("name", SchemaType.STRING),
                    Column("fruit", SchemaType.STRING),
                ]
            ),
        )
        def b(a):
            return a

        with pytest.raises(TypeError) as ex:
            b.run()
        assert (
            str(ex.value)
            == "Validation failure on node b when checking input node a: \n"
            '- Column name is of pandas type "object" but we are expecting type "<class \'numpy.int64\'>"\n'
            '- Column fruit is of pandas type "object" but we are expecting type "<class \'bool\'>"'
        )

    @pytest.mark.parametrize(
        "input_schema,is_error,expected_result",
        [
            (
                Schema(
                    [
                        Column("name", SchemaType.STRING),
                        Column("fruit", SchemaType.STRING),
                        Column("age", SchemaType.INTEGER),
                    ]
                ),
                True,
                "Validation failure on node c when checking input node a: \n"
                "- Column \"Column(name='age', type=<SchemaType.INTEGER: 3>)\" missing from dataframe",
            ),
            (
                Schema(
                    [
                        Column("name", SchemaType.STRING),
                    ]
                ),
                False,
                pd.DataFrame({"name": ["Albert", "Bob", "Chris", "Chris"]}),
            ),
        ],
    )
    def test_run_input_validation_column_mismatch(
        self, input_schema, is_error, expected_result
    ):
        """
        The output of transformation a is used as an input of b. If the input schema specified in b requests columns
        that a doesn't pass in then throw an appropriate error. However if the input schema specifies only a subset of
        the columns that a passes in then we just use the dataframe with that subset of columns selected.
        """

        @node(type="pandas")
        def a():
            return pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                }
            )

        @node(
            type="pandas",
            inputs=[
                (
                    a,
                    input_schema,
                ),
            ],
        )
        def c(a):
            return a

        if is_error:
            with pytest.raises(TypeError) as ex:
                c.run()
            assert str(ex.value) == expected_result
        else:
            assert_frame_equal(c.run(), expected_result)

    def test_run_output_validation_type_mismatch(self):
        """
        If the column types on the output schema do not match the actual column types then we throw an error.
        """

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("name", SchemaType.BOOLEAN),
                    Column("fruit", SchemaType.INTEGER),
                ]
            ),
        )
        def a():
            return pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                }
            )

        with pytest.raises(TypeError) as ex:
            a.run()
        assert (
            str(ex.value)
            == "Validation failure on node a when checking output schema: \n"
            '- Column name is of pandas type "object" but we are expecting type "<class \'bool\'>"\n'
            '- Column fruit is of pandas type "object" but we are expecting type "<class \'numpy.int64\'>"'
        )

    @pytest.mark.parametrize(
        "output_dataframe,expected_error",
        [
            (
                pd.DataFrame(
                    {
                        "name": ["Albert", "Bob", "Chris", "Chris"],
                    }
                ),
                "Validation failure on node a when checking output schema: \n"
                "- Column \"Column(name='fruit', type=<SchemaType.STRING: 1>)\" missing from dataframe",
            ),
            (
                pd.DataFrame(
                    {
                        "name": ["Albert", "Bob", "Chris", "Chris"],
                        "fruit": ["banana", "apple", "strawberry", "apple"],
                        "age": [20, 25, 30, 30],
                    }
                ),
                "Validation failure on node a when checking output schema: \n"
                "- Extra column \"Column(name='age', type=<SchemaType.STRING: 1>)\" found",
            ),
        ],
    )
    def test_run_output_validation_column_mismatch(
        self, output_dataframe, expected_error
    ):
        """
        If the transformation returns a dataframe with additional or missing columns from the schema then we throw an
        error.
        """

        @node(
            type="pandas",
            output=Schema(
                [
                    Column("name", SchemaType.STRING),
                    Column("fruit", SchemaType.STRING),
                ]
            ),
        )
        def a():
            return output_dataframe

        with pytest.raises(TypeError) as ex:
            a.run()
        assert str(ex.value) == expected_error
