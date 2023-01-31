import base64
import os
import requests
import re
import subprocess


RE_GITHUB_ISSUE = re.compile(r'#\d+')
GITHUB_URL = 'https://api.github.com/repos/flypipe/flypipe'
GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
github_connection = requests.session()


# Get commit list between HEAD and the most recent version number
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
    # release branch name will be origin/release/x.y.z, we remove the first 15 characters to just get the version
    latest_version = [int(num) for num in latest_version_branch_name[15:].split(".")]
    commit_list = (
        subprocess.check_output(
            f"git rev-list {latest_version_branch_name}..HEAD --no-merges", shell=True
        )
        .decode("utf-8")
        .split()
    )

issues = {}
for commit_id in commit_list:
    commit_message = subprocess.check_output(
        f"git show {commit_id} -s --format=%B", shell=True
    ).decode("utf-8")
    commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
    re_match = re.search(RE_GITHUB_ISSUE, commit_message_summary)
    if re_match:
        print(f'Found github issue {re_match.group(0)} in commit msg summary {commit_message_summary}')
        issue_id = re_match.group(0)[1:]
        issue = github_connection.get(f'{GITHUB_URL}/issues/{issue_id}', headers={'Authorization': f'Bearer {GITHUB_TOKEN}'}).json()
        if 'title' not in issue:
            print(f'Unable to find title for issue {issue_id}')
            continue
        issues[issue_id] = issue['title']


issue_ids = sorted(list(issues.keys()))
with open('changelog.txt', 'w', encoding='utf-8') as f:
    f.writelines(['Changelog\n', '*******\n'])
    lines = [f'- {issues[issue_id]}\n' for issue_id in issue_ids]
    f.writelines(lines)
