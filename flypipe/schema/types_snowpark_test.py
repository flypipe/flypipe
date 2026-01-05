import os
import pytest

from flypipe.schema.types import Date


@pytest.mark.skipif(
    os.environ.get("RUN_MODE") != "SNOWFLAKE",
    reason="Snowpark tests require RUN_MODE=SNOWFLAKE",
)
class TestDateSnowpark:
    """Tests for Date flypipe type - Snowpark
    
    Tests Snowflake-specific date format conversions.
    Reference: https://docs.snowflake.com/en/sql-reference/date-time-input-output
    """

    @pytest.mark.parametrize(
        "python_format,expected_snowflake_format",
        [
            ("%Y", "YYYY"),
            ("%y", "YY"),
            ("%b", "MON"),
            ("%B", "MMMM"),
            ("%m", "MM"),
            ("%a", "DY"),
            ("%A", "DY"),  # Snowflake doesn't distinguish between abbreviated and full day names
            ("%d", "DD"),
            ("%H", "HH24"),
            ("%I", "HH12"),
            ("%p", "AM"),
            ("%M", "MI"),
            ("%S", "SS"),
            ("%f", "FF"),
            ("%z", "TZHTZM"),  # Timezone offset
        ],
    )
    def test_convert_python_to_snowflake_datetime_format(
        self, python_format, expected_snowflake_format
    ):
        result = Date.convert_python_to_snowflake_datetime_format(python_format)
        assert result == expected_snowflake_format

    def test_convert_python_to_snowflake_datetime_format2(self):
        python_format = "%d/%m/%Y"
        expected_snowflake_format = "DD/MM/YYYY"
        result = Date.convert_python_to_snowflake_datetime_format(python_format)
        assert result == expected_snowflake_format

    def test_convert_python_to_snowflake_datetime_format_complex(self):
        python_format = "%Y-%m-%d %H:%M:%S"
        expected_snowflake_format = "YYYY-MM-DD HH24:MI:SS"
        result = Date.convert_python_to_snowflake_datetime_format(python_format)
        assert result == expected_snowflake_format

    def test_convert_python_to_snowflake_datetime_format_with_timezone(self):
        python_format = "%Y-%m-%d %H:%M:%S %z"
        expected_snowflake_format = "YYYY-MM-DD HH24:MI:SS TZHTZM"
        result = Date.convert_python_to_snowflake_datetime_format(python_format)
        assert result == expected_snowflake_format

    def test_convert_python_to_snowflake_datetime_format_unsupported(self):
        # %w (weekday as decimal) is not supported
        with pytest.raises(ValueError):
            Date.convert_python_to_snowflake_datetime_format("%w")
        
        # %j (day of year) is not supported in Snowflake
        with pytest.raises(ValueError):
            Date.convert_python_to_snowflake_datetime_format("%j")

    @pytest.mark.parametrize(
        "snowflake_format,expected_python_format",
        [
            ("YYYY", "%Y"),
            ("YY", "%y"),
            ("MON", "%b"),
            ("MMMM", "%B"),
            ("MM", "%m"),
            ("DY", "%a"),
            ("DD", "%d"),
            ("HH24", "%H"),
            ("HH12", "%I"),
            ("AM", "%p"),
            ("PM", "%p"),
            ("MI", "%M"),
            ("SS", "%S"),
            ("FF", "%f"),
            ("FF9", "%f"),
            ("FF6", "%f"),
            ("FF3", "%f"),
            ("FF0", "%f"),
            ("TZHTZM", "%z"),
            ("TZH:TZM", "%z"),
            ("TZH", "%z"),
        ],
    )
    def test_convert_snowflake_to_python_datetime_format(
        self, snowflake_format, expected_python_format
    ):
        result = Date.convert_snowflake_to_python_datetime_format(snowflake_format)
        assert result == expected_python_format

    def test_convert_snowflake_to_python_datetime_format2(self):
        snowflake_format = "DD/MM/YYYY"
        expected_python_format = "%d/%m/%Y"
        result = Date.convert_snowflake_to_python_datetime_format(snowflake_format)
        assert result == expected_python_format

    def test_convert_snowflake_to_python_datetime_format_complex(self):
        snowflake_format = "YYYY-MM-DD HH24:MI:SS"
        expected_python_format = "%Y-%m-%d %H:%M:%S"
        result = Date.convert_snowflake_to_python_datetime_format(snowflake_format)
        assert result == expected_python_format

    def test_convert_snowflake_to_python_datetime_format_with_timezone(self):
        snowflake_format = "YYYY-MM-DD HH24:MI:SS TZHTZM"
        expected_python_format = "%Y-%m-%d %H:%M:%S %z"
        result = Date.convert_snowflake_to_python_datetime_format(snowflake_format)
        assert result == expected_python_format

    def test_convert_snowflake_to_python_datetime_format_with_timezone_colon(self):
        snowflake_format = "YYYY-MM-DD HH24:MI:SS TZH:TZM"
        expected_python_format = "%Y-%m-%d %H:%M:%S %z"
        result = Date.convert_snowflake_to_python_datetime_format(snowflake_format)
        assert result == expected_python_format

