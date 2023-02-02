from jinja2 import Environment, PackageLoader, select_autoescape


def bla():
    ENV = Environment(
        loader=PackageLoader("flypipe"), autoescape=select_autoescape()
    )
    return ENV.get_template('index.html').render()

print(bla())