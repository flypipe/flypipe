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
sys.path.insert(0, os.path.abspath('../../'))


# -- Project information -----------------------------------------------------

project = 'FlyPipe'
copyright = f'2022, {project}'
author = 'Jose Helio de Brum Muller & Chris Marwick'

# The full version, including alpha/beta/rc tags
release = '0.0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    'sphinxcontrib.napoleon',
    'sphinx.ext.githubpages',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosectionlabel',
    'myst_parser',
    'sphinxcontrib.mermaid',
    'sphinx_substitution_extensions',
    'nbsphinx',
    'IPython.sphinxext.ipython_console_highlighting',
    'sphinx-favicon',
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
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    "setup.py",
    "**.ipynb_checkpoints",
    "**_test.py",
    "*config_test*"
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".

html_static_path = ['_static']
html_css_files = ['style.css']

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

suppress_warnings = ["myst.header"]

myst_substitutions = {
    'project': project
}

# html_context = {
#     "display_github": True,  # Integrate GitHub
#     "github_repo": "flypipe/flypipe",  # Repo name
#     "github_version": "feat/documentation",  # Version
#     "conf_py_path": "/docs/source/",  # Path in the checkout to the docs root
# }

html_favicon = "_static/favicon.svg"


# This is the expected signature of the handler for this event, cf doc
def autodoc_skip_member_handler(app, what, name, obj, skip, options):
    # Basic approach; you might want a regex instead

    # if what == "module":
    #     print(obj.name)

    if name == "node_graph_test":
        print("\n===========>", name)
        print(app)
        print(what, type(what))
        print(obj, type(obj))
        print(options)
        print(obj.__file__)
        print(obj.__package__)

    # if what == "module" and obj.__package__.endswith("_test"):
    #     return True

    # if what == "module" and obj.__package__.startswith("flypipe.tests"):
    #     return True
    #
    #
    #
    # if what == "module" and name.startswith("Test"):
    #     return True

    return False


# Automatically called by sphinx at startup
def setup(app):
    # Connect the autodoc-skip-member event from apidoc to the callback
    app.connect('autodoc-skip-member', autodoc_skip_member_handler)
