from jinja2 import Environment, PackageLoader, select_autoescape


ENV = None


def _get_env():
    global ENV
    if not ENV:
        ENV = Environment(
            loader=PackageLoader("flypipe"), autoescape=select_autoescape()
        )
    return ENV


def get_template(template_name):
    env = _get_env()
    return env.get_template(template_name)
