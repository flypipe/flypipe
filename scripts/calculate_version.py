"""
This script calculates the next version number which Flypipe ought to release to. Flypipe uses semantic versioning
(https://semver.org/) for its releases, and a simplified form of conventional commits where the each commit has the form:
<commit type>: <summary>
We leverage these to get the appropriate version for new releases in the following process:
1. Get the most recent release
2. Look at the commit type in the list of commits between HEAD and the last released version.
    - If there is an exclamation mark denoting a breaking change after the type aka "feat!: ..." then we
    increment the major version on the most recent released version and the resultant version number is our new release
    number.
    - If none of the commits are breaking changes but we have at least one feature change (i.e feat: ...) then we
    increment the minor version on the most recent released version and the resultant version number is our new release
    number.
    - If neither of the above two events occurred then we increment the patch version on the most recent released
    version and the resultant version number is our new release number.
"""
import sys
import re
import os
from typing import List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.utils import get_release_branches, get_commit_list, get_commit_message

RE_COMMIT_TYPE = r'(\w+!?):'

def get_changes(re_commit_type, commit_message):

    is_breaking_change, is_feature_change = False, False
    commit_type_match = re.search(re_commit_type, commit_message)
    if commit_type_match:
        # print(' *** commit contains a type')
        commit_type_str = commit_type_match.group(1)
        if commit_type_str[-1] == '!':
            # print(' *** detected breaking change')
            is_breaking_change = True
        elif commit_type_str == "feat":
            # print(' *** detected feature')
            is_feature_change = True

    return is_breaking_change, is_feature_change


def calculate_version(to_branch: str = None, re_commit_type: str = None) -> List[str]:
    to_branch = to_branch or "HEAD"
    re_commit_type = re_commit_type or RE_COMMIT_TYPE

    latest_version = [0, 0, 0]
    release_branches = get_release_branches()
    # print(f'Check diff for {to_branch}')
    if not release_branches:
        # Unable to find a previous release
        commit_list = get_commit_list(to_branch=to_branch)
    else:
        latest_version_branch_name = max(release_branches)
        latest_version = [int(num) for num in latest_version_branch_name[15:].split(".")]
        commit_list = get_commit_list(from_branch=latest_version_branch_name, to_branch=to_branch)

    new_version = latest_version
    if not commit_list:
        raise Exception("Release would be made without any commits, aborting")

    is_breaking_change = False
    is_feature_change = False
    # print('*** Start checking through list of commits ***')
    for commit_id in commit_list:
        commit_message = get_commit_message(commit_id)
        # print(f'Checking msg {commit_message} (commit id {commit_id})')
        commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
        # print(f'Check commit "{commit_message_summary}"')
        is_breaking_change_, is_feature_change_ = get_changes(re_commit_type, commit_message_summary)
        if is_breaking_change_:
            is_breaking_change = True
            break

        if is_feature_change_:
            is_feature_change = True
    # print('*** Finished checking through list of commits ***')

    if is_breaking_change:
        # print('Conclusion- breaking change, incrementing major version')
        new_version[0] += 1
        new_version[1] = 0
        new_version[2] = 0
    elif is_feature_change:
        # print('Conclusion- backwards compatible feature change, incrementing minor version')
        new_version[1] += 1
        new_version[2] = 0
    else:
        # print('Conclusion- minor change, incrementing patch version')
        new_version[2] += 1

    return new_version

def save_version(new_version):
    new_version_string = ".".join([str(num) for num in new_version])
    with open('flypipe/version.txt', 'w') as f:
        f.write(new_version_string)

if __name__ == '__main__':
    commands = sys.argv[1:]
    to_branch = "HEAD" if not commands else commands[0]
    new_version = calculate_version(to_branch)
    save_version(new_version)
    print("Version is",'.'.join([str(v) for v in new_version]))

