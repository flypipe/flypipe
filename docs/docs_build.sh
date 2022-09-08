#!/bin/sh

make clean
rm -r ./source/modules
mkdir ./source/modules
sphinx-apidoc -f -o source/modules ../flypipe
python git.py
make html