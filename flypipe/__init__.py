"""
Flypipe
"""
import os

with open(
    os.path.join(os.path.dirname(__file__), "version.txt"), "r", encoding="utf-8"
) as f:
    __version__ = f.read()
# TODO: we shouldn't need this try except, there's some weird thing that happens when we run flit build where it runs
#  this module to get the version for the wheel file and cannot find the package flypipe. Skip for now.
try:
    from flypipe.node import node  # noqa: F401
    from flypipe.node_function import node_function  # noqa: F401
except:  # noqa: E722
    pass
