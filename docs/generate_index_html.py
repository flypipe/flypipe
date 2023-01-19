"""
Sphinx multiversion builds a subfolder for each version of the docs that we build. We need to build a quick little
index.html that redirects to the latest version of the docs, otherwise going to the index page for the documentation
gives a 404 error.
"""

import os

import jinja2

os.chdir(os.path.dirname(os.path.realpath(__file__)))
REPO_NAME = "flypipe"


def get_redirect_url():
    if os.path.exists('./build/html/main'):
        return './html/main/index.html'
    else:
        doc_versions = os.listdir('./build/html/release')
        version = max(doc_versions)
        return f'./html/release/{version}/index.html'


environment = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath="./"))
template = environment.get_template("./index.html")
with open("./build/index.html", "w") as f:
    f.write(template.render(redirect_url=get_redirect_url()))
