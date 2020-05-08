#!/usr/bin/env python3
import json
import gzip

with gzip.open("testdata.ldj.gz","wt") as outp:
    for n in range(0,1000):
        print(json.dumps({"foo":n,
                          "bar":1000-n,
                          "baz":"test{}".format(n)
                         }), file=outp)
