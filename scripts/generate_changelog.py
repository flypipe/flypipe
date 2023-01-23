import base64
import os
import requests
import re
import subprocess


BASE_URL = 'https://prospa.atlassian.net/rest/api/3'
RE_JIRA_ID = r'\w+-\d+'
JIRA_USER = os.environ.get('JIRA_USER')
JIRA_TOKEN = os.environ.get('JIRA_TOKEN')
jira_connection = requests.session()
auth_string = base64.b64encode(f'{JIRA_USER}:{JIRA_TOKEN}'.encode('utf-8')).decode('utf-8')


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
    latest_version = [int(num) for num in latest_version_branch_name[15:].split(".")]
    commit_list = (
        subprocess.check_output(
            f"git rev-list {latest_version_branch_name}..HEAD --no-merges", shell=True
        )
        .decode("utf-8")
        .split()
    )

features = {}
bugs = {}
for commit_id in commit_list:
    commit_message = subprocess.check_output(
        f"git show {commit_id} -s --format=%B", shell=True
    ).decode("utf-8")
    commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
    re_match = re.search(RE_JIRA_ID, commit_message_summary)
    if re_match:
        jira_id = re_match.group(0)
        if jira_id in features or jira_id in bugs:
            continue
        issue_data = requests.get(
            f'{BASE_URL}/issue/{jira_id}',
            headers={
                'Authorization': f'Basic {auth_string}',
                'Content-Type': 'application/json'
            }).json()
        issue_type = issue_data['fields']['issuetype']['name']
        if issue_type == 'Story':
            print(f'Found feature with id {jira_id}')
            features[jira_id] = issue_data['fields']['summary']
        elif issue_type == 'Bug':
            print(f'Found bugfix with id {jira_id}')
            bugs[jira_id] = issue_data['fields']['summary']
        else:
            print(f'Issue {jira_id} has type {issue_type}, ignoring')

feature_summaries = sorted(features.values())
bug_summaries = sorted(bugs.values())
with open('changelog.txt', 'w', encoding='utf-8') as f:
    f.writelines(['Changelog\n', '*******\n'])
    if features:
        f.writelines(['Features: \n'])
        f.writelines(f'- {feature}\n' for feature in feature_summaries)
    if bugs:
        f.writelines(['Bugs: \n'])
        f.writelines(f'- {bug}\n' for bug in bug_summaries)
