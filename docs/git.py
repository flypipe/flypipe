import requests
from pprint import pprint
import os

headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {os.environ.get('GIT_TOKEN')}"
}

contributors = requests.get('https://api.github.com/repos/flypipe/flypipe/stats/contributors', headers=headers).json()
contributors = sorted(contributors, key=lambda x: -x['total'])

contributions = ""
with open(os.path.join(os.getcwd(), "source/readmes/contributors.md"), "w") as f:

    for contributor in contributors:
        f.write(f"""\n- [@{contributor['author']['login']}]({contributor['author']['html_url']}) {contributor['total']} commits""")


f.close()