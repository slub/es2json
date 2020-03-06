#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import elasticsearch
import argparse
import logging
import sys
import os
from httplib2 import Http  # needed for put_dict


def isint(num):
    try:
        int(num)
        return True
    except (ValueError, TypeError):
        return False


def isfloat(num):
    try:
        float(num)
        return True
    except (ValueError, TypeError):
        return False


def isiter(obj):
    try:
        _ = (e for e in obj)
        return True
    except TypeError:
        return False


def isfile(path):
    try:
        return os.path.isfile(path)
    except TypeError:
        return False


def put_dict(url, dictionary):
    """
    Pass the whole dictionary as a json body to the url.
    Make sure to use a new Http object each time for thread safety.
    """
    http_obj = Http()
    resp, content = http_obj.request(
        uri=url,
        method='PUT',
        headers={'Content-Type': 'application/json'},
        body=json.dumps(dictionary),
    )


def ArrayOrSingleValue(array):
    if isinstance(array, (int, float)):
        return array
    if array:
        length = len(array)
        if length > 1 or isinstance(array, dict):
            return array
        elif length == 1:
            for elem in array:
                return elem
        elif length == 0:
            return None


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def eprintjs(*args, **kwargs):
    for arg in args:
        print(json.dumps(arg, indent=4), file=sys.stderr, **kwargs)


def esfatgenerator(host=None, port=9200, index=None, type=None, chunksize=1000, body=None, source=True, source_exclude=None, source_include=None, timeout=10):
    if not source:
        source = True
    es = elasticsearch.Elasticsearch([{'host': host}], port=port)
    try:
        if elasticsearch.VERSION < (7, 0, 0):
            page = es.search(
                index=index,
                doc_type=type,
                scroll='12h',
                size=chunksize,
                body=body,
                _source=source,
                _source_exclude=source_exclude,
                _source_include=source_include,
                request_timeout=timeout)
        elif elasticsearch.VERSION >= (7, 0, 0):
            page = es.search(
                index=index,
                scroll='12h',
                size=chunksize,
                body=body,
                _source=source,
                _source_excludes=source_exclude,
                _source_includes=source_include,
                request_timeout=timeout)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("aborting.\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    yield page.get('hits').get('hits')
    while (scroll_size > 0):
        pages = es.scroll(scroll_id=sid, scroll='12h')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        yield pages.get('hits').get('hits')

#   returns records which have a certain ID from an ID-File from an elasticsearch-index
#   IDs in the ID-File shall be non-quoted, newline-seperated
#


def esgenerator(host=None, port=9200, index=None, type=None, id=None, chunksize=1000, body=None, source=True, source_exclude=None, source_include=None, headless=False, timeout=10, verbose=False):
    progress = chunksize
    if not source:
        source = True
    es = elasticsearch.Elasticsearch(
        [{'host': host}], port=port, timeout=timeout, max_retries=10, retry_on_timeout=True)
    try:
        if id:
            if elasticsearch.VERSION < (7, 0, 0):
                record = es.get(index=index, doc_type=type, id=id)
            elif elasticsearch.VERSION >= (7, 0, 0):
                record = es.get(index=index, id=id)
            if headless:
                yield record["_source"]
            else:
                yield record
            return
        if elasticsearch.VERSION < (7, 0, 0):
            page = es.search(
                index=index,
                doc_type=type,
                scroll='12h',
                size=chunksize,
                body=body,
                _source=source,
                _source_exclude=source_exclude,
                _source_include=source_include)
        # no doc_type and slightly different _source parameters in elasticsearch7
        elif elasticsearch.VERSION >= (7, 0, 0):
            page = es.search(
                index=index,
                scroll='12h',
                size=chunksize,
                body=body,
                _source=source,
                _source_excludes=source_exclude,
                _source_includes=source_include)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("not found: "+host+":"+str(port) +
                         "/"+index+"/"+type+"/_search\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    for hits in page['hits']['hits']:
        if headless:
            yield hits['_source']
        else:
            yield hits
    while (scroll_size > 0):
        pages = es.scroll(scroll_id=sid, scroll='12h')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        if verbose:
            eprint("{}/{}".format(progress, pages['hits']['total']))
            progress += chunksize
        for hits in pages['hits']['hits']:
            if headless:
                yield hits['_source']
            else:
                yield hits


def esidfilegenerator(host=None, port=9200, index=None, type=None, body=None, source=True, source_exclude=None, source_include=None, idfile=None, headless=False, chunksize=1000, timeout=10):
    if not source:
        source = True
    tracer = logging.getLogger('elasticsearch')
    tracer.setLevel(logging.WARNING)
    tracer.addHandler(logging.FileHandler('errors.txt'))
    es = elasticsearch.Elasticsearch(
        [{'host': host}], port=port, timeout=timeout, max_retries=10, retry_on_timeout=True)
    ids = set()

    if isinstance(idfile, str) and isfile(idfile):
        with open(idfile, "r") as inp:
            for ppn in inp:
                _id = ppn.rstrip()
                ids.add(_id)
    elif isiter(idfile) and not isinstance(idfile, str) and not isfile(idfile):
        for ppn in idfile:
            ids.add(ppn.rstrip())
    if len(ids) >= chunksize:
        if body and "query" in body and "match" in body["query"]:
            searchbody = {
                "query": {"bool": {"must": [{"match": body["query"]["match"]}, {}]}}}
            for _id in ids:
                searchbody["query"]["bool"]["must"][1] = {
                    "match": {"_id": _id}}
                # eprint(json.dumps(searchbody))
                for doc in esgenerator(host=host, port=port, index=index, type=type, body=searchbody, source=source, source_exclude=source_exclude, source_include=source_include, headless=False, timeout=timeout, verbose=False):
                    if headless:
                        yield doc.get("_source")
                    else:
                        yield doc
            ids.clear()
        else:
            searchbody = {'ids': list(ids)}
            try:
                if elasticsearch.VERSION < (7, 0, 0):
                    for doc in es.mget(index=index, doc_type=type, body=searchbody, _source_include=source_include, _source_exclude=source_exclude, _source=source).get("docs"):
                        if headless:
                            yield doc.get("_source")
                        else:
                            yield doc
                # no doc_type and slightly different _source parameters in elasticsearch7
                elif elasticsearch.VERSION >= (7, 0, 0):
                    for doc in es.mget(index=index, body=searchbody, _source_includes=source_include, _source_excludes=source_exclude, _source=source).get("docs"):
                        if headless:
                            yield doc.get("_source")
                        else:
                            yield doc
                ids.clear()
            except elasticsearch.exceptions.NotFoundError as e:
                traceback.print_exc()
        if len(ids) > 0:
            if body and "query" in body and "match" in body["query"]:
                searchbody = {
                    "query": {"bool": {"must": [{"match": body["query"]["match"]}, {}]}}}
                for _id in ids:
                    searchbody["query"]["bool"]["must"][1] = {
                        "match": {"_id": _id}}
                    # eprint(json.dumps(searchbody))
                    for doc in esgenerator(host=host, port=port, index=index, type=type, body=searchbody, source=source, source_exclude=source_exclude, source_include=source_include, headless=False, timeout=timeout, verbose=False):
                        if headless:
                            yield doc.get("_source")
                        else:
                            yield doc
                ids.clear()
            else:
                searchbody = {'ids': list(ids)}
                try:
                    if elasticsearch.VERSION < (7, 0, 0):
                        for doc in es.mget(index=index, doc_type=type, body=searchbody, _source_include=source_include, _source_exclude=source_exclude, _source=source).get("docs"):
                            if headless:
                                yield doc.get("_source")
                            else:
                                yield doc
                    # no doc_type and slightly different _source parameters in elasticsearch7
                    elif elasticsearch.VERSION >= (7, 0, 0):
                        for doc in es.mget(index=index, body=searchbody, _source_includes=source_include, _source_excludes=source_exclude, _source=source).get("docs"):
                            if headless:
                                yield doc.get("_source")
                            else:
                                yield doc
                    ids.clear()
                except elasticsearch.exceptions.NotFoundError:
                    pass

#   returns records which have a certain ID from an ID-File from an elasticsearch-index
#   IDs in the ID-File shall be non-quoted, newline-seperated
#   "consumes" the file, which means if it runs clean, the file will be deleted. if some errors occure, only the IDs which arent downloaded get preserved
#


def esidfileconsumegenerator(host=None, port=9200, index=None, type=None, body=None, source=True, source_exclude=None, source_include=None, idfile=None, headless=False, chunksize=1000, timeout=10):
    if isfile(idfile):
        ids = list()
        notfound_ids = set()
        with open(idfile, "r") as inp:
            for ppn in inp:
                ids.append(ppn.rstrip())
        if not source:
            source = True
        tracer = logging.getLogger('elasticsearch')
        tracer.setLevel(logging.WARNING)
        tracer.addHandler(logging.FileHandler('errors.txt'))
        es = elasticsearch.Elasticsearch(
            [{'host': host}], port=port, timeout=timeout, max_retries=10, retry_on_timeout=True)
        success = False
        _ids = set()
        try:
            for _id in ids:
                _ids.add(ids.pop())
                if len(_ids) >= chunksize:
                    if elasticsearch.VERSION < (7, 0, 0):
                        for doc in es.mget(index=index, doc_type=type, body={'ids': list(_ids)}, _source_include=source_include, _source_exclude=source_exclude, _source=source).get("docs"):
                            if headless:
                                yield doc.get("_source")
                            else:
                                yield doc
                    # no doc_type and slightly different _source parameters in elasticsearch7
                    elif elasticsearch.VERSION >= (7, 0, 0):
                        for doc in es.mget(index=index, body={'ids': list(_ids)}, _source_includes=source_include, _source_excludes=source_exclude, _source=source).get("docs"):
                            if headless:
                                yield doc.get("_source")
                            else:
                                yield doc
                    _ids.clear()
            if len(_ids) > 0:
                if elasticsearch.VERSION < (7, 0, 0):
                    for doc in es.mget(index=index, doc_type=type, body={'ids': list(_ids)}, _source_include=source_include, _source_exclude=source_exclude, _source=source).get("docs"):
                        if headless:
                            yield doc.get("_source")
                        else:
                            yield doc
                # no doc_type and slightly different _source parameters in elasticsearch7
                elif elasticsearch.VERSION >= (7, 0, 0):
                    for doc in es.mget(index=index, body={'ids': list(_ids)}, _source_includes=source_include, _source_excludes=source_exclude, _source=source).get("docs"):
                        if headless:
                            yield doc.get("_source")
                        else:
                            yield doc
                _ids.clear()
                ids.clear()
        except elasticsearch.exceptions.NotFoundError:
            notfound_ids.add(_ids)
        else:
            os.remove(idfile)
        finally:
            ids += notfound_ids
            with open(idfile, "w") as outp:
                for _id in ids:
                    print(_id, file=outp)

    # avoid dublettes and nested lists when adding elements into lists


def litter(lst, elm):
    if not lst:
        return elm
    else:
        if isinstance(elm, (str, dict)):
            if isinstance(lst, list) and elm in lst:
                return lst
            else:
                if isinstance(lst, (dict, str)):
                    return [lst, elm]
                elif isinstance(lst, list):
                    lst.append(elm)
                    return lst
        elif isinstance(elm, list):
            if isinstance(lst, str):
                lst = [lst]
            if isinstance(lst, list):
                for element in elm:
                    if element not in lst:
                        lst.append(element)
            return lst
        else:
            return lst


def run():
    parser = argparse.ArgumentParser(description='simple ES.Getter!')
    parser.add_argument('-host', type=str, default="127.0.0.1",
                        help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port', type=int, default=9200,
                        help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index', type=str,
                        help='ElasticSearch Search Index to use')
    parser.add_argument('-type', type=str,
                        help='ElasticSearch Search Index Type to use')
    parser.add_argument('-source', type=str, help='just return this field(s)')
    parser.add_argument("-include", type=str,
                        help="include following _source field(s)")
    parser.add_argument("-exclude", type=str,
                        help="exclude following _source field(s)")
    parser.add_argument(
        "-id", type=str, help="retrieve single document (optional)")
    parser.add_argument("-headless", action="store_true",
                        default=False, help="don't include Elasticsearch Metafields")
    parser.add_argument('-body', type=json.loads, help='Searchbody')
    # no, i don't steal the syntax from esbulk...
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty")
    parser.add_argument(
        '-idfile', type=str, help="path to a file with newline-delimited IDs to process")
    parser.add_argument('-idfile_consume', type=str,
                        help="path to a file with newline-delimited IDs to process")
    parser.add_argument('-pretty', action="store_true",
                        default=False, help="prettyprint")
    parser.add_argument('-chunksize', type=int, default=1000, help=
                        "chunksize of the search window to use")
    args = parser.parse_args()
    if args.server:
        slashsplit = args.server.split("/")
        args.host = slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port = args.server.split(":")[2].split("/")[0]
        args.index = args.server.split("/")[3]
        if len(slashsplit) > 4:
            args.type = slashsplit[4]
        if len(slashsplit) > 5:
            if "?pretty" in args.server:
                args.pretty = True
                args.id = slashsplit[5].rsplit("?")[0]
            else:
                args.id = slashsplit[5]
    if args.pretty:
        tabbing = 4
    else:
        tabbing = None
    if args.idfile:
        for json_record in esidfilegenerator(host=args.host, port=args.port, index=args.index, type=args.type, body=args.body, source=args.source, headless=args.headless, source_exclude=args.exclude, source_include=args.include, idfile=args.idfile):
            sys.stdout.write(json.dumps(json_record, indent=tabbing)+"\n")
    elif args.idfile_consume:
        for json_record in esidfileconsumegenerator(host=args.host, port=args.port, index=args.index, type=args.type, body=args.body, source=args.source, headless=args.headless, source_exclude=args.exclude, source_include=args.include, idfile=args.idfile_consume):
            sys.stdout.write(json.dumps(json_record, indent=tabbing)+"\n")
    elif not args.id:
        for json_record in esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, body=args.body, source=args.source, headless=args.headless, source_exclude=args.exclude, source_include=args.include, verbose=True):
            sys.stdout.write(json.dumps(json_record, indent=tabbing)+"\n")
    else:
        es = elasticsearch.Elasticsearch([{"host": args.host}], port=args.port)
        json_record = None
        if not args.headless and elasticsearch.VERSION < (7, 0, 0):
            json_record = es.get(index=args.index, doc_type=args.type, _source=True,
                                 _source_exclude=args.exclude, _source_include=args.include, id=args.id)
        elif not args.headless and elasticsearch.VERSION > (7, 0, 0):
            json_record = es.get(index=args.index, _source=True,
                                 _source_excludes=args.exclude, _source_includes=args.include, id=args.id)
        elif elasticsearch.VERSION < (7, 0, 0):
            json_record = es.get_source(index=args.index, doc_type=args.type, _source=True,
                                        _source_exclude=args.exclude, _source_include=args.include, id=args.id)
        elif elasticsearch.VERSION > (7, 0, 0):
            json_record = es.get_source(
                index=args.index, _source=True, _source_excludes=args.exclude, _source_includes=args.include, id=args.id)
        if json_record:
            sys.stdout.write(json.dumps(json_record, indent=tabbing)+"\n")


if __name__ == "__main__":
    run()
