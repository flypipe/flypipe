import IPython

ipython = IPython.get_ipython()
if 'ipython' in globals():
    ipython.magic('load_ext autoreload')
    ipython.magic('autoreload 2')


def displayHTML(html):
    from html import escape
    html_to_display = f"""
    <iframe 
        srcdoc="{escape(html)}" 
        width="100%" 
        height=1000"
        scrolling="no"
        style="border: none; overflow:hidden; overflow-y:hidden;"
    ></iframe>
    """
    return IPython.display.HTML(html_to_display)
