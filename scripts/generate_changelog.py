import os
import pathlib
import requests
import re
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.utils import get_changelog_latest_branch_release
from scripts.utils import get_release_branches, get_commit_list
from scripts.calculate_version import get_commit_message, calculate_version

RE_GITHUB_ISSUE = re.compile(r'#\d+')
GITHUB_URL = 'https://api.github.com/repos/flypipe/flypipe'
GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
github_connection = requests.session()

def git_get(url):
    return github_connection.get(url, headers={'Authorization': f'Bearer {GITHUB_TOKEN}'}).json()

def generate_changelog(to_branch: str=None):
    to_branch = to_branch or "HEAD"
    # Get commit list between HEAD and the most recent version number
    release_branches = get_release_branches()
    if not release_branches:
        # Unable to find a previous release
        commit_list = get_commit_list(to_branch=to_branch)
    else:
        latest_version_branch_name = max(release_branches)
        commit_list = get_commit_list(from_branch=latest_version_branch_name, to_branch=to_branch)

    issues = {}
    for commit_id in commit_list:
        commit_message = get_commit_message(commit_id)
        commit_message_summary = commit_message.split("\n", maxsplit=1)[0]
        re_match = re.search(RE_GITHUB_ISSUE, commit_message_summary)
        if re_match:
            # print(f'Found github issue {re_match.group(0)} in commit msg summary {commit_message_summary}')
            issue_id = re_match.group(0)[1:]
            url = f'{GITHUB_URL}/issues/{issue_id}'
            issue = git_get(url)
            if 'title' not in issue:
                # print(f'Unable to find title for issue {issue_id}')
                continue
            issues[issue_id] = f'<a href="https://github.com/flypipe/flypipe/issues/{issue_id}" target="_blank" rel="noopener noreferrer">{issue_id} {issue["title"]}</a>'

    return issues


def save_changelog(issues, version):
    lines = ["Changelog", "\n========="]
    issue_ids = sorted(list(issues.keys()), reverse=True)
    if issue_ids:
        version = '.'.join([str(v) for v in version])
        version = f'<h2><a href="https://github.com/flypipe/flypipe/tree/release/{version}" target="_blank" rel="noopener noreferrer">release/{version}</a><h2>'
        lines += [f"\n\n{version}\n\n"] + [f'- {issues[issue_id]}\n' for issue_id in issue_ids]

    changelog_lines = get_changelog_latest_branch_release()
    lines = lines + (changelog_lines or [])

    file_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), "changelog.md")

    with open(file_path, 'w') as file:
        file.writelines(lines)

    return lines

if __name__ == '__main__':
    import sys
    commands = sys.argv[1:]
    to_branch = "HEAD" if not commands else commands[0]
    issues = generate_changelog(to_branch)

    version = calculate_version(to_branch=to_branch)
    contents = save_changelog(issues, version)

    print("".join(contents))