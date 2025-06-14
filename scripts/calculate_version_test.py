import re
from unittest import mock

import pytest

from scripts.calculate_version import get_changes, RE_COMMIT_TYPE, get_release_branches, calculate_version, \
    get_commit_list


class TestCalculateVersion:

    @pytest.mark.parametrize(
        "message, expected_is_breaking_change, expected_is_feature_change",
        [
            ("msg", False, False),
            ("fix: msg", False, False),
            ("fix!: msg", True, False),
            ("feat: msg", False, True),
            ("feat!: msg", True, False),
            ("#173 feat!: msg", True, False),
            (" feat!: msg", True, False),
            ("feat! msg", False, False),
            ("feat msg", False, False),
        ]
    )
    def test_version(self, message, expected_is_breaking_change, expected_is_feature_change):
        is_breaking_change, is_feature_change = get_changes(RE_COMMIT_TYPE, message)
        assert (is_breaking_change, is_feature_change) == (expected_is_breaking_change, expected_is_feature_change)

    def test_release_branches(self):
        for r in get_release_branches():
            assert re.compile(r'^origin/release/\d+\.\d+\.\d+$').match(r).group(0) == r

    def test_get_commit_list_of_existing_old_branch_returns_non_empty_list(self):
        release_branches = get_release_branches()
        if release_branches:
            first_release_branch = min(release_branches)
            assert len(get_commit_list(first_release_branch)) > 0

    def test_commit_message(self):
        release_branches = get_release_branches()
        if release_branches:
            first_release_branch = min(release_branches)
            commit_list = get_commit_list(first_release_branch)
            assert len(commit_list) > 0

    @pytest.mark.parametrize(
        "message,expected_version",
        [
            ("msg", [0, 0, 1]),
            ("fix: msg", [0, 0, 1]),
            ("fix!: msg", [1, 0, 0]),
            ("feat: msg", [0, 1, 0]),
            ("feat!: msg", [1, 0, 0]),
            (" feat!: msg", [1, 0, 0]),
            ("feat! msg", [0, 0, 1]),
            ("feat msg", [0, 0, 1]),

            ("#123 msg", [0, 0, 1]),
            ("#123 fix: msg", [0, 0, 1]),
            ("#123 fix!: msg", [1, 0, 0]),
            ("#123 feat: msg", [0, 1, 0]),
            ("#123 feat!: msg", [1, 0, 0]),
            ("#123  feat!: msg", [1, 0, 0]),
            ("#123 feat! msg", [0, 0, 1]),
            ("#123 feat msg", [0, 0, 1]),
        ]
    )
    def test_calculate_version(self, message, expected_version):
        with (
            mock.patch('scripts.calculate_version.get_release_branches', return_value=["origin/release/0.0.0"]),
            mock.patch('scripts.calculate_version.get_commit_list', return_value=["1"]),
            mock.patch('scripts.calculate_version.get_commit_message', return_value=message)
        ):
            assert calculate_version() == expected_version

