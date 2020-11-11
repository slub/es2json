#!/usr/bin/env python3
import json
import gzip

from test_es2json_esfunctions import MAX

with gzip.open("testdata.ldj.gz", "wt") as outp:
    for n in range(0, MAX):
        print(json.dumps({"foo": n,
                          "bar": MAX-n,
                          "baz": "test{}".format(n)
                          }), file=outp)
