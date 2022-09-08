#!/bin/sh

make clean
python git.py
rm -r ./source/modules
mkdir ./source/modules
sphinx-apidoc -f -o source/modules ../flypipe
make html