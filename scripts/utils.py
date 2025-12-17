import subprocess
import requests
import base64

def get_github_file_content(branch, file_path, owner="flypipe", repo="flypipe"):
    """
    Fetches the content of a file from a specific branch in a GitHub repo.

    Args:
        owner (str): GitHub username or organization.
        repo (str): Repository name.
        branch (str): Branch name.
        file_path (str): Path to the file within the repo.
        token (str, optional): GitHub personal access token for private repos or higher rate limits.

    Returns:
        str: Content of the file as a string.
    """
    url = (f"https://api.github.com/repos/{owner}/{repo}/contents/{file_path}")
    headers = {'Accept': 'application/vnd.github.v3+json'}
    params = {'ref': branch}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        content = response.json().get('content')
        return base64.b64decode(content).decode('utf-8')
    else:
        raise Exception(f"Failed to fetch file: {response.status_code} {response.text}")


def get_release_branches():
    all_branches = (
        subprocess.check_output('git for-each-ref --format="%(refname:short)"', shell=True)
        .decode("utf-8")
        .split()
    )
    release_branches = [
        branch for branch in all_branches if branch.startswith("origin/release/")
    ]

    release_branches = sorted(release_branches)[-10:]

    return release_branches

def get_commit_list(from_branch=None, to_branch=None):
    to_branch=to_branch or "HEAD"
    # if from_branch:
    #     print(f'Check diff between {from_branch} and {to_branch}')

    from_branch = f"{from_branch}.." if from_branch else ""
    git_cmd = f"git rev-list {from_branch}{to_branch} --no-merges"
    commit_list = (
        subprocess.check_output(git_cmd, shell=True)
        .decode("utf-8")
        .split()
    )

    return commit_list

def get_commit_message(commit_id):
    commit_message = subprocess.check_output(
        f"git show {commit_id} -s --format=%B", shell=True
    ).decode("utf-8")

    return commit_message

def get_changelog_latest_branch_release():
    release_branches = get_release_branches()
    if release_branches:
        latest_version_branch_name = max(release_branches)
        lines = get_github_file_content(latest_version_branch_name.replace("origin/", ""), "changelog.md", owner="flypipe", repo="flypipe")
        lines = lines.splitlines()[2:]
        lines = [l for l in lines if l.strip() != ""]
        return lines