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
                    "age": [30, 25, 28, 28],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
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

    def test_run_input_validation_missing_column(self):
        """
        Throw an error if a column is requested in the schema but not provided in the input dataframe
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
                            Column("name", SchemaType.STRING),
                            Column("fruit", SchemaType.STRING),
                            Column("age", SchemaType.INTEGER),
                        ]
                    ),
                ),
            ],
        )
        def b(a):
            return a

        with pytest.raises(TypeError) as ex:
            b.run()
        assert str(ex.value) == (
            "Validation failure on node b when checking input node a: \n"
            "- Column \"Column(name='age', type=<SchemaType.INTEGER: 3>)\" missing from "
            "dataframe"
        )

    def test_run_input_validation_extra_column(self):
        """
        Exclude any columns provided in the input dataframe that aren't requested in the schema.
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
                            Column("name", SchemaType.STRING),
                        ]
                    ),
                ),
            ],
        )
        def b(a):
            return a

        assert_frame_equal(
            b.run(),
            pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                }
            ),
        )

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

    def test_run_output_validation_missing_column(self):
        """
        Throw an error if a column is requested in the schema but not provided in the output dataframe.
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
            return pd.DataFrame(
                    {
                        "name": ["Albert", "Bob", "Chris", "Chris"],
                    }
                )

        with pytest.raises(TypeError) as ex:
            a.run()
        assert str(ex.value) == (
            "Validation failure on node a when checking output schema: \n"
            "- Column \"Column(name='fruit', type=<SchemaType.STRING: 1>)\" missing from dataframe"
        )

    def test_run_output_validation_extra_column(self):
        """
        Exclude any columns provided in the output dataframe that aren't requested in the schema.
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
            return pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                    "age": [20, 25, 30, 30],
                }
            )

        assert_frame_equal(
            a.run(),
            pd.DataFrame(
                {
                    "name": ["Albert", "Bob", "Chris", "Chris"],
                    "fruit": ["banana", "apple", "strawberry", "apple"],
                }
            ),
        )
