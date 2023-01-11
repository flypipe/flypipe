"""
Get the diff between the latest release and the current HEAD, using the conventional commit rules we can
calculate the offset from the latest release and thus the new version (assuming the current state of HEAD were to be
released)
"""

import subprocess

print(subprocess.check_output('git branch', shell=True))
all_branches = subprocess.check_output('git for-each-ref --format="%(refname:short)" refs/heads/', shell=True).decode('utf-8').split()
release_branches = [branch for branch in all_branches if branch.startswith('release/')]
if not release_branches:
    # Unable to find a previous release
    latest_version = [0, 0, 0]
    commit_list = subprocess.check_output(f'git rev-list HEAD --no-merges', shell=True).decode('utf-8').split()
else:
    latest_version_branch_name = max(release_branches)
    latest_version = [int(num) for num in latest_version_branch_name[8:].split('.')]
    commit_list = subprocess.check_output(f'git rev-list {latest_version_branch_name}..HEAD --no-merges', shell=True).decode('utf-8').split()

new_version = latest_version
if not commit_list:
    raise Exception('Release would be made without any commits, aborting')
is_breaking_change = False
is_feature_change = False
for commit_id in commit_list:
    commit_message = subprocess.check_output(f'git show {commit_id} -s --format=%B', shell=True).decode('utf-8')
    commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
    if 'BREAKING_CHANGE' in commit_message:
        is_breaking_change = True
    elif commit_message.startswith('feat:'):
        is_feature_change = True

if is_breaking_change:
    new_version[0] += 1
elif is_feature_change:
    new_version[1] += 1
else:
    new_version[2] += 1

new_version_string = '.'.join([str(num) for num in new_version])
print(new_version_string)
