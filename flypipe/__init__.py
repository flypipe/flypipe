"""
Flypipe
"""
with open('version.txt', 'r') as f:
    __version__ = f.read()
from flypipe.node import node
from flypipe.node_function import node_function
