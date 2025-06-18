#!/bin/sh

cd docs
make clean
rm -r source/modules
mkdir source/modules
cd ..
#sphinx-apidoc -f -o source/modules ../flypipe ../flypipe/**test.py
python docs/build_docs.py
python docs/generate_index_html.py
#make html