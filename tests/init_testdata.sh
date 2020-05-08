#!/bin/sh
# needs esbulk and elasticsearch running
./generate_testdata
esbulk -server http://localhost:9200 -index test -type _doc -id foo -w 1 -verbose -z testdata.ldj.gz
