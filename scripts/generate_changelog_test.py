from unittest import mock
import pytest
from scripts.generate_changelog import generate_changelog


class TestGenerateChangeLog:

    @pytest.mark.parametrize(
        "message,expected_message",
        [
            ("#173 msg", {'173': '<a href="https://github.com/flypipe/flypipe/issues/173" target="_blank" rel="noopener noreferrer">173 abc</a>'}),
        ]
    )
    def test_calculate_version(self, message, expected_message):
        with (
            mock.patch('scripts.generate_changelog.get_release_branches', return_value=["origin/release/0.0.0"]),
            mock.patch('scripts.generate_changelog.get_commit_list', return_value=["1"]),
            mock.patch('scripts.generate_changelog.get_commit_message', return_value=message),
            mock.patch('scripts.generate_changelog.git_get', return_value={"title": "abc"})
        ):
            issues = generate_changelog()
            assert issues == expected_message



