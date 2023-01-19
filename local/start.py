# noinspection PyUnresolvedReferences
import warnings

import IPython

from flypipe.tests.spark import spark

warnings.filterwarnings("ignore")

ipython = IPython.get_ipython()
if "ipython" in globals():
    ipython.magic("load_ext autoreload")
    ipython.magic("autoreload 2")

html_width = "100%"
html_height = "1000"


def displayHTML(html):
    from html import escape

    html_to_display = f"""
    <iframe 
        srcdoc="{escape(html)}" 
        width="{html_width}" 
        height="{html_height}"
        scrolling="no"
        style="border: none; overflow:hidden; overflow-y:hidden;"
    ></iframe>
    """
    return IPython.display.HTML(html_to_display)
