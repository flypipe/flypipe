#!/bin/sh

cd docs
make clean
rm -r source/modules
mkdir source/modules
sphinx-apidoc -f -o source/modules ../flypipe ../flypipe/**test.py
make html