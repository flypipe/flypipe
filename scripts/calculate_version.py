"""
This script calculates the next version number which Flypipe ought to release to. Flypipe uses semantic versioning
(https://semver.org/) for it's releases, and conventional commits (https://www.conventionalcommits.org/en/v1.0.0/) for
the commit format. We leverage these to get the appropriate version for new releases in the following process:
1. Get the most recent release
2. Scan through the commit messages of the list of commits between the most recent release and HEAD:
    - If we encounter any commit with BREAKING CHANGE or ! after the type then we increment the major version on the
    most recent released version and the resultant version number is our new release number.
    - If none of the commits contained BREAKING CHANGE or ! after the type but we have at least one feature change (i.e
    type == 'feat') then we increment the minor version on the most recent released version and the resultant version
    number is our new release number.
    - If neither of the above two events occurred then we increment the patch version on the most recent released
    version and the resultant version number is our new release number.

Get the diff between the latest release and the current HEAD, using the conventional commit rules we can
calculate the offset from the latest release and thus the new version (assuming the current state of HEAD were to be
released)
"""

import subprocess
import re

RE_COMMIT_TYPE = r'(^\w+!?):'

all_branches = (
    subprocess.check_output('git for-each-ref --format="%(refname:short)"', shell=True)
    .decode("utf-8")
    .split()
)
release_branches = [
    branch for branch in all_branches if branch.startswith("origin/release/")
]
if not release_branches:
    # Unable to find a previous release
    latest_version = [0, 0, 0]
    commit_list = (
        subprocess.check_output(f"git rev-list HEAD --no-merges", shell=True)
        .decode("utf-8")
        .split()
    )
else:
    latest_version_branch_name = max(release_branches)
    latest_version = [int(num) for num in latest_version_branch_name[15:].split(".")]
    commit_list = (
        subprocess.check_output(
            f"git rev-list {latest_version_branch_name}..HEAD --no-merges", shell=True
        )
        .decode("utf-8")
        .split()
    )

new_version = latest_version
if not commit_list:
    raise Exception("Release would be made without any commits, aborting")
is_breaking_change = False
is_feature_change = False
for commit_id in commit_list:
    commit_message = subprocess.check_output(
        f"git show {commit_id} -s --format=%B", shell=True
    ).decode("utf-8")
    commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
    commit_type_match = re.search(RE_COMMIT_TYPE, commit_message_summary)
    if commit_type_match:
        commit_type_str = commit_type_match.group(1)
        if commit_type_str[-1] == '!':
            is_breaking_change = True
        elif commit_type_str == "feat":
            is_feature_change = True

if is_breaking_change:
    new_version[0] += 1
elif is_feature_change:
    new_version[1] += 1
else:
    new_version[2] += 1

new_version_string = ".".join([str(num) for num in new_version])
print(new_version_string)
