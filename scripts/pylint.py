import subprocess

import os

print(os.getcwd())
subprocess.call('python -m pylint ../flypipe')
