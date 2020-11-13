#!/bin/sh
# needs esbulk and elasticsearch running
./tests/generate_testdata.py
esbulk -server http://localhost:9200 -index test -type _doc -id foo -w 1 -verbose -z tests/testdata.ldj.gz
