"""
This script calculates the next version number which Flypipe ought to release to. Flypipe uses semantic versioning
(https://semver.org/) for its releases, and a simplified form of conventional commits where the each commit has the form:
<commit type>: <summary>
We leverage these to get the appropriate version for new releases in the following process:
1. Get the most recent release
2. Look at the commit type in the list of commits between HEAD and the last released version.
    - If there is an exclamation mark denoting a breaking change after the type aka "feat!: ..." or "fix!: ..." then we
    increment the major version on the most recent released version and the resultant version number is our new release
    number.
    - If none of the commits are breaking changes but we have at least one feature change (i.e feat: ...) then we
    increment the minor version on the most recent released version and the resultant version number is our new release
    number.
    - If neither of the above two events occurred then we increment the patch version on the most recent released
    version and the resultant version number is our new release number.
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
    print(f'Check diff for HEAD')
    commit_list = (
        subprocess.check_output(f"git rev-list HEAD --no-merges", shell=True)
        .decode("utf-8")
        .split()
    )
else:
    latest_version_branch_name = max(release_branches)
    latest_version = [int(num) for num in latest_version_branch_name[15:].split(".")]
    print(f'Check diff between {latest_version_branch_name} and HEAD')
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
print('*** Start checking through list of commits ***')
for commit_id in commit_list:
    commit_message = subprocess.check_output(
        f"git show {commit_id} -s --format=%B", shell=True
    ).decode("utf-8")
    print(f'Checking msg {commit_message} (commit id {commit_id})')
    commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
    print(f'Check commit "{commit_message_summary}"')
    commit_type_match = re.search(RE_COMMIT_TYPE, commit_message_summary)
    if commit_type_match:
        print(' *** commit contains a type')
        commit_type_str = commit_type_match.group(1)
        if commit_type_str[-1] == '!':
            print(' *** detected breaking change')
            is_breaking_change = True
        elif commit_type_str == "feat":
            print(' *** detected feature')
            is_feature_change = True
print('*** Finished checking through list of commits ***')

if is_breaking_change:
    print('Conclusion- breaking change, incrementing major version')
    new_version[0] += 1
    new_version[1] = 0
    new_version[2] = 0
elif is_feature_change:
    print('Conclusion- backwards compatible feature change, incrementing minor version')
    new_version[1] += 1
    new_version[2] = 0
else:
    print('Conclusion- minor change, incrementing patch version')
    new_version[2] += 1

new_version_string = ".".join([str(num) for num in new_version])
with open('flypipe/version.txt', 'w') as f:
    f.write(new_version_string)
