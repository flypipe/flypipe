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
    lines = ["Changelog\n=========\n"]

    breaking_changes = {
        "5.0.0": [
            "\n\n<h3>Breaking Changes</h3>\n\n",
            "- **Removed `parallel` argument from `node.run()`**: Use `max_workers` parameter instead to control parallel execution. Set `max_workers=1` for sequential execution or `max_workers=N` for parallel execution with N workers. You can also set `FLYPIPE_NODE_RUN_MAX_WORKERS` environment variable to configure this globally (defaults to `1`).\n\n",
            "- **Removed `FLYPIPE_DEFAULT_RUN_MODE` config**: This config previously defined parallel or sequential execution mode. Now use `FLYPIPE_NODE_RUN_MAX_WORKERS` instead to control parallelism. Set it to `1` for sequential execution or `N` for parallel execution with N workers. This can be set globally via environment variable or passed directly to `node.run()` as the `max_workers` parameter.\n\n",
            "- **Updated `Cache` class method signatures**: The `read()` and `write()` methods now include optional CDC (Change Data Capture) parameters:\n"
            "\t- `read(from_node=None, to_node=None, *args, **kwargs)`: Added `from_node` and `to_node` parameters for CDC filtering\n"
            "\t- `write(*args, df, upstream_nodes=None, to_node=None, datetime_started_transformation=None, **kwargs)`: Added `df`, `upstream_nodes`, `to_node`, and `datetime_started_transformation` parameters for CDC metadata tracking\n"
            "\t- All custom cache implementations must update their method signatures to include these parameters, even if CDC functionality is not used (parameters can be ignored)\n",
            "- **Added `create_cdc_table()` method to `Cache` class**: A new optional method for caches with CDC support. The base implementation is a no-op, so existing cache implementations will continue to work, but cache classes that support CDC should override this method to create their CDC metadata tables.\n\n"
        ]
    }

    issue_ids = sorted(list(issues.keys()), reverse=True)
    if issue_ids:
        version = '.'.join([str(v) for v in version])
        breaking_changes_lines = breaking_changes.get(version, [])
        breaking_changes_lines += ["<h3>Commits</h3>"] if breaking_changes_lines else []
        version = f'<h2><a href="https://github.com/flypipe/flypipe/tree/release/{version}" target="_blank" rel="noopener noreferrer">release/{version}</a></h2>'
        lines += [version] + breaking_changes_lines + [f'* {issues[issue_id]}' for issue_id in issue_ids]

    changelog_lines = get_changelog_latest_branch_release()
    lines = lines + (changelog_lines or [])

    for i, line in enumerate(lines):
        if line.startswith('<h2>'):
            lines[i] = "\n\n" + line

        if line.startswith('*'):
            lines[i] = "\n" + line

        if line.endswith('</h2>'):
            lines[i] += "\n"

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