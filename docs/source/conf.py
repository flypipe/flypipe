# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.append(os.path.abspath("./_ext"))
sys.path.insert(0, os.path.abspath("../../"))
os.chdir("../..")

from flypipe import __version__
from datetime import datetime

today = datetime.today()
# -- Project information -----------------------------------------------------

project = "Flypipe"
copyright = f"{today.year}, {project}"
author = "Jose Helio de Brum Muller & Chris Marwick"

# The full version, including alpha/beta/rc tags
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinxcontrib.images",
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinxcontrib.napoleon",
    "sphinx.ext.githubpages",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "myst_parser",
    "sphinx_multiversion",
    "sphinx_substitution_extensions",
    "nbsphinx",
    "IPython.sphinxext.ipython_console_highlighting",
    "sphinx-favicon",
]

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# Display todos by setting to True
myst_all_links_external = True
myst_number_code_blocks = ["python"]
# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

html_sidebars = {
    "**": [
        "versions.html",
    ],
}

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    "setup.py",
    "**.ipynb_checkpoints",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".

html_static_path = ["_static"]
html_css_files = ["style.css"]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

suppress_warnings = ["myst.header"]

myst_substitutions = {
    "project": project,
}

# html_context = {
#     "display_github": True,  # Integrate GitHub
#     "github_repo": "flypipe/flypipe",  # Repo name
#     "github_version": "feat/documentation",  # Version
#     "conf_py_path": "/docs/source/",  # Path in the checkout to the docs root
# }

html_favicon = "_static/favicon.svg"

smv_branch_whitelist = r"^release/"

# Whitelist pattern for remotes (set to None to use local branches only)
smv_remote_whitelist = r"^.*$"

# Determines whether remote or local git branches/tags are preferred if their output dirs conflict
smv_prefer_remote_refs = True
