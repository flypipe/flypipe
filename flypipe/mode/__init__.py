# ruff: noqa: F401
"""
This is a utils package to easy the import of modes.

Instead of doing:
from flypipe.cache import CacheMode
from flypipe.dependency.preprocess_mode import PreprocessMode

This package gives and alternative to the imports as

from flypipe.mode import CacheMode
from flypipe.mode import PreprocessMode
"""
from flypipe.cache import CacheMode
from flypipe.dependency.preprocess_mode import PreprocessMode
