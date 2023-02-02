import os
from jinja2 import Environment, PackageLoader, select_autoescape


def bla():
    ENV = Environment(
        loader=PackageLoader("flypipe"), autoescape=select_autoescape()
    )
    print(f'cwd: {os.getcwd()}')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path, 'js/bundle.js'), 'r') as f:
        js_bundle = f.read()
    return ENV.get_template('index.html').render(js_bundle=js_bundle)
