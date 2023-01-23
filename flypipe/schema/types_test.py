import pytest

from flypipe.schema.types import Date


class TestDate:
    """Tests for Date flypipe type"""

    @pytest.mark.parametrize(
        "python_format,expected_pyspark_format",
        [
            ("%Y", "yyyy"),
            ("%y", "yy"),
            ("%j", "yyy"),
            ("%b", "MMM"),
            ("%B", "MMMM"),
            ("%m", "MM"),
            ("%a", "E"),
            ("%A", "EEEE"),
            ("%d", "dd"),
            ("%H", "H"),
            ("%I", "h"),
            ("%p", "a"),
            ("%M", "m"),
            ("%S", "s"),
            ("%f", "SSSSSS"),
            ("%z", "z"),
        ],
    )
    def test_convert_python_to_pyspark_datetime_format(
        self, python_format, expected_pyspark_format
    ):
        result = Date.convert_python_to_pyspark_datetime_format(python_format)
        assert result == expected_pyspark_format

    def test_convert_python_to_pyspark_datetime_format2(self):
        python_format = "%d/%m/%Y"
        expected_pyspark_format = "dd/MM/yyyy"
        result = Date.convert_python_to_pyspark_datetime_format(python_format)
        assert result == expected_pyspark_format

    def test_convert_python_to_pyspark_datetime_format_unsupported(self):
        with pytest.raises(ValueError):
            Date.convert_python_to_pyspark_datetime_format("%w")

    @pytest.mark.parametrize(
        "pyspark_format,expected_python_format",
        [
            ("yyyy", "%Y"),
            ("yy", "%y"),
            ("yyy", "%j"),
            ("MMM", "%b"),
            ("MMMM", "%B"),
            ("MM", "%m"),
            ("E", "%a"),
            ("EEEE", "%A"),
            ("dd", "%d"),
            ("H", "%H"),
            ("h", "%I"),
            ("a", "%p"),
            ("m", "%M"),
            ("s", "%S"),
            ("SSSSSS", "%f"),
            ("z", "%z"),
        ],
    )
    def test_convert_pyspark_to_python_datetime_format(
        self, pyspark_format, expected_python_format
    ):
        result = Date.convert_pyspark_to_python_datetime_format(pyspark_format)
        assert result == expected_python_format

    def test_convert_pyspark_to_python_datetime_format2(self):
        pyspark_format = "dd/MM/yyyy"
        expected_python_format = "%d/%m/%Y"
        result = Date.convert_pyspark_to_python_datetime_format(pyspark_format)
        assert result == expected_python_format
