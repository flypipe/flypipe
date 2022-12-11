from jinja2 import Environment, PackageLoader, select_autoescape


_env = None


def _get_env():
    global _env
    if not _env:
        _env = Environment(
            loader=PackageLoader("flypipe.printer"), autoescape=select_autoescape()
        )
    return _env


def get_template(template_name):
    env = _get_env()
    return env.get_template(template_name)
