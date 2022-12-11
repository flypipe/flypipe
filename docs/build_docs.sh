#!/bin/sh

make clean
rm -r ./source/modules
mkdir ./source/modules
#sphinx-apidoc -f -o source/modules ../flypipe ../flypipe/**test.py
#sphinx-multiversion -f -o source/modules ../flypipe ../flypipe/**test.py --dump-metadata
sphinx-multiversion ./source ./build/html
#sphinx-multiversion ./source ./build/html -f -o source/modules ../flypipe ../flypipe/**test.py
make html