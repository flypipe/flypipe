import re
from unittest import mock

import pytest

from scripts.utils import get_release_branches, get_commit_list


class TestUtils:

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

