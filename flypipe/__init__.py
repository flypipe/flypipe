"""
Flypipe
"""
import os
with open(os.path.join(os.path.dirname(__file__), 'version.txt'), 'r') as f:
    __version__ = f.read()
from flypipe.node import node
from flypipe.node_function import node_function