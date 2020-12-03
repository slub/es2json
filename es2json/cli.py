#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import json
import es2json.helperscripts as helperscripts
from es2json import ESGenerator
from es2json import IDFile
from es2json import IDFileConsume

def run():
    """
    here We build a simple cmdline tool so we can use our classes from shell
    """
    parser = argparse.ArgumentParser(description='Query elasticsearch indices/index/documents and print them '
                                                 'formatted as JSON-Objects',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-server', type=str, help="use http://host:port/index/type/id.\n"
                        "host:port - hostname or IP with port of the elasticsearch node to query\n"
                        "            default: localhost:9200\n"
                        "index     - index to query\n"
                        "            default: None → queries across all available indices\n"
                        "type      - elasticsearch doctype to use (optional)\n"
                        "id        - identifier of one specific document to query (optional)",
                        default="http://127.0.0.1:9200")
    parser.add_argument('-source', action="store_true", default=True,
                        help='return the Document or just the Elasticsearch-Metadata')
    parser.add_argument('-size', type=str, default=None, metavar="N[:M]",
                        help='just return the first n-Records of the search,\n'
                        'or return a python slice, e.g. 2:10 returns a list\n'
                        'from the 2nd including the 9th element of the search\n'
                        'only works with the ESGenerator\n'
                        'Note: Not all slice variants may be supported')
    parser.add_argument('-timeout', type=int, default=10,
                        help='Set the time in seconds after when a ReadTimeoutError can occur.\n'
                        'Default is 10 seconds. Raise for big/difficult querys ')
    parser.add_argument("-includes", type=str,
                        help="just include following _source field(s) in the _source object")
    parser.add_argument("-excludes", type=str,
                        help="exclude following _source field(s) from the _source object")
    parser.add_argument("-headless", action='store_true', default=False,
                        help="don't print Elasticsearch metadata")
    parser.add_argument('-body', type=helperscripts.jsonstring_or_file,
                        help='Elasticsearch Query object that can be in the form of\n'
                        '1) a JSON string (e.g. \'{"query": {"match": {"name": "foo"}}}\')\n'
                        '2) a file containing the upper query string')
    parser.add_argument('-idfile', type=str,
                        help="path to a file with \\n-delimited IDs to process")
    parser.add_argument('-idfile_consume', type=str,
                        help="path to a file with \\n-delimited IDs to process")
    parser.add_argument('-pretty', action='store_true', default=False,
                        help="prettyprint the json output")
    parser.add_argument('-verbose', action='store_true', default=False,
                        help="print progress for large dumps")
    parser.add_argument('-chunksize', type=int, default=1000,
                        help="chunksize of the search window to use")
    args = parser.parse_args()

    #parsing server                             # http://server.de:1234/index/_doc/101
    slashsplit = args.server.split("/")         # → [http:, , server.de:1234, index, _doc, 101]
    args.host = slashsplit[2].rsplit(":")[0]
    args.port = int(args.server.split(":")[2].rsplit("/")[0]) # raise Error if port not castable to int
    if len(slashsplit) > 3:
        args.index = slashsplit[3]
    if len(slashsplit) > 4:
        args.type_ = slashsplit[4]
    if len(slashsplit) > 5:
        args.id_ = slashsplit[5]
    if args.size:
        """
        we build the slice() object here, if this fails because of user input,
        the stacktrace of slice() is very informative, so we don't do our own Error handling here
        for size-searches, we don't use a scroll since the user wants only a small searchwindow
        """
        if isinstance(args.size, int):  # oh, we got an single number, not a string with an number or even an string describing a slice
            args.size = str(args.size)
        if ':' in args.size:
            args.slice_ = slice(int(args.size.split(':')[0]), int(args.size.split(':')[1]), 1)
        else:
            args.slice_ = slice(0, int(args.size), 1)
    if args.headless and not args.source:
        helperscripts.eprint("ERROR! do not use -headless and -source False at the same Time!")
        exit(-1)
    if args.pretty:
        tabbing = 4
    else:
        tabbing = None
    kwargs_generator = dict(**vars(args))
    for item in ("includes", "excludes"):  # str2list
        if isinstance(kwargs_generator.get(item), str):
            kwargs_generator[item] = kwargs_generator.pop(item).split(",")
    kwargs_generator.pop("server")
    kwargs_generator.pop("pretty")
    kwargs_generator.pop("size")           # translated to args.slice
    if args.idfile:
        ESGeneratorFunction = IDFile(**kwargs_generator).generator()
    elif args.idfile_consume:
        kwargs_generator["idfile"] = kwargs_generator.pop("idfile_consume")
        ESGeneratorFunction = IDFileConsume(**kwargs_generator).generator()
    else:
        kwargs_generator.pop("idfile")
        kwargs_generator.pop("idfile_consume")
        ESGeneratorFunction = ESGenerator(**kwargs_generator).generator()
    for json_record in ESGeneratorFunction:
        print(json.dumps(json_record, indent=tabbing))


if __name__ == "__main__":
    run()
