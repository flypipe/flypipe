import os.path
import subprocess


def get_release_branches():
    all_branches = (
        subprocess.check_output('git for-each-ref --format="%(refname:short)"', shell=True)
        .decode("utf-8")
        .split()
    )
    release_branches = [
        branch for branch in all_branches if branch.startswith("origin/release/")
    ]

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

def prepend_lines_to_file(file_path, lines_to_prepend, version):
    """
    Prepend a list of lines to the beginning of a file.

    Args:
        file_path (str): Path to the target file.
        lines_to_prepend (list[str]): Lines to insert at the beginning.
    """
    original_content = []

    if os.path.exists(file_path):
        with open(file_path, 'r') as original:
            original_content = original.readlines()

    if original_content:
        original_content = original_content[2:]


    version = '.'.join([str(v) for v in version])
    version = f'<h2><a href="https://github.com/flypipe/flypipe/tree/release/{version}" target="_blank" rel="noopener noreferrer">release/{version}</a><h2>'
    lines_to_prepend = [f"\n\n{version}\n\n"] + lines_to_prepend

    contents = lines_to_prepend + original_content
    contents = ["Changelog\n=========\n"] + contents
    with open(file_path, 'w') as modified:
        modified.writelines(contents)

    return contents