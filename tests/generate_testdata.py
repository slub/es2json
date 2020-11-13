#!/usr/bin/env python3
import json
import gzip

MAX = 1000

if __name__ == "__main__":
    with gzip.open("./tests/testdata.ldj.gz", "wt") as outp:
        for n in range(0, MAX):
            print(json.dumps({"foo": n,
                            "bar": MAX-n,
                            "baz": "test{}".format(n)
                            }), file=outp)
