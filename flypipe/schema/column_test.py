import pytest

from flypipe import node
from flypipe.config import config_context
from flypipe.schema import Schema, Column
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
