#!/bin/sh

make clean
rm -r ./source/modules
mkdir ./source/modules
#sphinx-apidoc -f -o source/modules ../flypipe ../flypipe/**test.py
python build_docs.py
python generate_index_html.py
#make html