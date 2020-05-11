#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import elasticsearch
import argparse
import logging
import sys
import traceback
import os
from httplib2 import Http  # needed for put_dict


def isint(num):
    '''
    check if num is a int without throwing an exception
    returns True/False
    '''
    try:
        int(num)
        return True
    except (ValueError, TypeError):
        return False


def isfloat(num):
    '''
    check if num is a float without throwing an exception
    returns True/False
    '''
    try:
        float(num)
        return True
    except (ValueError, TypeError):
        return False


def isiter(obj):
    '''
    check if obj is iterable without throwing an exception
    returns True/False
    '''
    try:
        _ = (e for e in obj)
        return True
    except TypeError:
        return False


def isfile(path):
    '''
    check if path is file without throwing an exception
    returns True/False
    '''
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
    '''
    return an array
    if there is only a single value, only return that single value
    '''
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
    '''
    print to stderr
    '''
    print(*args, file=sys.stderr, **kwargs)


def eprintjs(*args, **kwargs):
    '''
    pretty print dicts and arrays as json to stderr
    '''
    for arg in args:
        print(json.dumps(arg, indent=4), file=sys.stderr, **kwargs)


class ES_wrapper:
    """ wraps functionality of the python elasticsearch client used in lod-api

        In Order to properly react on different elasticsearch versions
        this wrapper manages the difference in function calls to the es api
    """
    @staticmethod
    def call(es, action, **kwargs):
        """ Call a method of the elasticsearch api on a specified index
        with multiple variable kwargs as options to each call. """
        server_version = int(es.info()['version']['number'][0])
        client_version = elasticsearch.VERSION[0]
        if server_version < 7 and client_version < 7:
            if '_source_excludes' in kwargs:
                kwargs['_source_exclude'] = kwargs.pop('_source_excludes')
            if '_source_includes' in kwargs:
                kwargs['_source_include'] = kwargs.pop('_source_includes')
        return getattr(es, action)(**kwargs)

    @staticmethod
    def get_mapping_props(es, index, doc_type=None):
        """ Requests the properties of a mapping applied to one index """
        server_version = int(es.info()['version']['number'][0])
        mapping = es.indices.get_mapping(index=index)
        if server_version < 7 and doc_type:
            return mapping[index]["mappings"][doc_type]["properties"]
        elif server_version >= 7:
            return mapping[index]["mappings"]["properties"]


def esfatgenerator(host=None,
                   port=9200,
                   index=None,
                   type=None,
                   chunksize=1000,
                   body=None,
                   source=True,
                   source_excludes=None,
                   source_includes=None,
                   timeout=10):
    '''
    dumps elasticsearch indices (or restricted by search body),
    dumps whole search pages per yield
    useful for further use by multiprocessesed tasks
    '''
    if not source:
        source = True
    es = elasticsearch.Elasticsearch(
        [{
          'host': host,
          'port': port,
          'timeout': timeout,
          'max_retries': 10,
          'retry_on_timeout': True,
          'http_compress': True
        }]
        )
    server_version = es.info()['version']['number']
    try:
        page = ES_wrapper.call(es,
                               'search',
                               index=index,
                               doc_type=type,
                               scroll='12h',
                               size=chunksize,
                               body=body,
                               source=source,
                               _source_excludes=source_excludes,
                               _source_includes=source_includes,
                               request_timeout=timeout)
        if int(server_version[0]) < 7:
            scroll_size = page['hits']['total']
        elif int(server_version[0]) >= 7:
            scroll_size = page['hits']['total']["value"]
    except elasticsearch.exceptions.NotFoundError:
        eprint("not found: {h}:{p}/{i}/{t}/_search"
               .format(h=host, p=port, i=index, t=type))
        exit(-1)
    sid = page['_scroll_id']
    yield page['hits']['hits']
    while (scroll_size > 0):
        pages = ES_wrapper.call(es,
                                'scroll',
                                scroll_id=sid,
                                scroll='12h')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        yield pages['hits']['hits']


def esgenerator(host=None,
                port=9200,
                index=None,
                type=None,
                id=None,
                chunksize=1000,
                body=None,
                source=True,
                source_excludes=None,
                source_includes=None,
                headless=False,
                timeout=10,
                verbose=False):
    '''
    dumps elasticsearch indices (or restricted by search body),
    dumps single records per yield
    '''
    progress = chunksize
    if not source:
        source = True
    es = elasticsearch.Elasticsearch(
        [{
          'host': host,
          'port': port,
          'timeout': timeout,
          'max_retries': 10,
          'retry_on_timeout': True,
          'http_compress': True
        }]
        )
    server_version = es.info()['version']['number']
    try:
        if id:
            record = ES_wrapper.call(es,
                                     'get',
                                     index=index,
                                     doc_type=type,
                                     id=id,
                                     _source_excludes=source_excludes,
                                     _source_includes=source_includes)
            if headless:
                yield record["_source"]
            else:
                yield record
            return
        page = ES_wrapper.call(es,
                               'search',
                               index=index,
                               doc_type=type,
                               scroll='12h',
                               size=chunksize,
                               body=body,
                               _source=source,
                               _source_excludes=source_excludes,
                               _source_includes=source_includes)
        if int(server_version[0]) < 7:
            scroll_size = page['hits']['total']
        elif int(server_version[0]) >= 7:
            scroll_size = page['hits']['total']["value"]
    except elasticsearch.exceptions.NotFoundError:
        eprint("not found: {h}:{p}/{i}/{t}/_search"
               .format(h=host, p=port, i=index, t=type))
        exit(-1)
    sid = page['_scroll_id']
    for hits in page['hits']['hits']:
        if headless:
            yield hits['_source']
        else:
            yield hits
    while (scroll_size > 0):
        pages = ES_wrapper.call(es,
                                'scroll',
                                scroll_id=sid,
                                scroll='12h')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        if int(server_version[0]) < 7:
            total_size = page['hits']['total']
        elif int(server_version[0]) >= 7:
            total_size = page['hits']['total']["value"]
        if verbose:
            eprint("{}/{}".format(progress, total_size))
            progress += chunksize
        for hits in pages['hits']['hits']:
            if headless:
                yield hits['_source']
            else:
                yield hits


def esidfilegenerator(host=None,
                      port=9200,
                      index=None,
                      type=None,
                      body=None,
                      source=True,
                      source_excludes=None,
                      source_includes=None,
                      idfile=None,
                      headless=False,
                      chunksize=1000,
                      timeout=10):
    '''
    dumps elasticsearch records, defined by an idfile or iterable object
    dumps single records per yield
    TODO: implement usage of functioning search querys, atm its very limited
    '''
    if not source:
        source = True
    tracer = logging.getLogger('elasticsearch')
    tracer.setLevel(logging.WARNING)
    tracer.addHandler(logging.FileHandler('errors.txt'))
    es = elasticsearch.Elasticsearch(
        [{
          'host': host,
          'port': port,
          'timeout': timeout,
          'max_retries': 10,
          'retry_on_timeout': True,
          'http_compress': True
        }]
        )
    ids_set = set()
    ids = list()

    if isinstance(idfile, str) and isfile(idfile):
        with open(idfile, "r") as inp:
            for ppn in inp:
                _id = ppn.rstrip()
                ids_set.add(_id)
    elif isiter(idfile) and not isinstance(idfile, str) and not isfile(idfile):
        for ppn in idfile:
            ids_set.add(ppn.rstrip())
    ids = list(ids_set)
    while len(ids) >= chunksize:
        if body and "query" in body and "match" in body["query"]:
            searchbody = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": body["query"]["match"]
                            },
                            {
                                "match": {}
                            }
                        ]
                    }
                }
            }
            for _id in ids[:chunksize]:
                searchbody["query"]["bool"]["must"][1]["match"] = {"_id": _id}
                eprint(json.dumps(searchbody))
                for doc in esgenerator(host=host,
                                       port=port,
                                       index=index,
                                       type=type,
                                       body=searchbody,
                                       source=source,
                                       source_excludes=source_excludes,
                                       source_includes=source_includes,
                                       headless=False,
                                       timeout=timeout,
                                       verbose=False):
                    if headless:
                        yield doc.get("_source")
                    else:
                        yield doc
            del ids[:chunksize]
        else:
            searchbody = {'ids': ids[:chunksize]}
            try:
                docs = ES_wrapper.call(es,
                                       'mget',
                                       index=index,
                                       doc_type=type,
                                       body=searchbody,
                                       _source_includes=source_includes,
                                       _source_excludes=source_excludes,
                                       _source=source)["docs"]
                for doc in docs:
                    if headless:
                        yield doc["_source"]
                    else:
                        yield doc
                del ids[:chunksize]
            except elasticsearch.exceptions.NotFoundError:
                traceback.print_exc()
    while len(ids) > 0:
        if body and "query" in body and "match" in body["query"]:
            searchbody = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": body["query"]["match"]
                            },
                            {
                                "match": {}
                            }
                        ]
                    }
                }
            }
            for _id in ids:
                searchbody["query"]["bool"]["must"][1]["match"] = {"_id": _id}
                # eprint(json.dumps(searchbody))
                for doc in esgenerator(host=host,
                                       port=port,
                                       index=index,
                                       type=type,
                                       body=searchbody,
                                       source=source,
                                       source_excludes=source_excludes,
                                       source_includes=source_includes,
                                       headless=False,
                                       timeout=timeout,
                                       verbose=False):
                    if headless and doc["found"]:
                        yield doc["_source"]
                    elif doc["found"]:
                        yield doc
                    else:
                        eprint("not found: {h}:{p}/{i}/{t}/{id}"
                               .format(h=host,
                                       p=port,
                                       i=doc['_index'],
                                       t=doc['_type'],
                                       id=doc['_id']))
            del ids[:]
        else:
            searchbody = {'ids': ids}
            try:
                docs = ES_wrapper.call(es,
                                       'mget',
                                       index=index,
                                       doc_type=type,
                                       body=searchbody,
                                       _source_includes=source_includes,
                                       _source_excludes=source_excludes,
                                       _source=source)["docs"]
                for doc in docs:
                    if headless and doc["found"]:
                        yield doc["_source"]
                    elif doc["found"]:
                        yield doc
                    else:
                        eprint("not found: {h}:{p}/{i}/{t}/{id}"
                               .format(h=host,
                                       p=port,
                                       i=doc['_index'],
                                       t=doc['_type'],
                                       id=doc['_id']))
                    del ids[:]
            except elasticsearch.exceptions.NotFoundError:
                eprint("not found: {h}:{p}/{i}/{t}/_search"
                       .format(h=host, p=port, i=index, t=type))


def esidfileconsumegenerator(host=None,
                             port=9200,
                             index=None,
                             type=None,
                             body=None,
                             source=True,
                             source_excludes=None,
                             source_includes=None,
                             idfile=None,
                             headless=False,
                             chunksize=1000,
                             timeout=10):
    '''
    dumps elasticsearch records, defined by an idfile
    dumps single records per yield
    consumes the idfile, if all records are successfull transceived,
    the idfile is going to be empty. if there is an error inbetween,
    the idfile is going to be partly used for the successfull pages which are
    printed, if the error occurs inbetween a page, the page will not be
    successfull printed out completely, but will be purged from the idfile.
    reduce chunksize parameter to reduce this error. but lesser chunksize is
    also lesser performance
    TODO: implement usage of functioning search querys, atm its very limited
    '''
    if isfile(idfile):
        ids = set()
        notfound_ids = set()
        with open(idfile, "r") as inp:
            for ppn in inp:
                ids.add(ppn.rstrip())
        list_ids = list(ids)
        if not source:
            source = True
        tracer = logging.getLogger('elasticsearch')
        tracer.setLevel(logging.WARNING)
        tracer.addHandler(logging.FileHandler('errors.txt'))
        es = elasticsearch.Elasticsearch([{'host': host,
                                           'port': port,
                                           'timeout': timeout,
                                           'max_retries': 10,
                                           'retry_on_timeout': True,
                                           'http_compress': True
                                           }])
        try:
            while len(list_ids) >= chunksize:
                docs = ES_wrapper.call(es,
                                       'mget',
                                       index=index,
                                       doc_type=type,
                                       body={'ids': list_ids[:chunksize]},
                                       _source_includes=source_includes,
                                       _source_excludes=source_excludes,
                                       _source=source)["docs"]
                for doc in docs:
                    if headless and doc["found"]:
                        yield doc.get("_source")
                    elif doc["found"]:
                        yield doc
                    elif not doc["found"]:
                        notfound_ids.add(doc["_id"])
                del list_ids[:chunksize]
            if len(list_ids) > 0:
                docs = ES_wrapper.call(es,
                                       'mget',
                                       index=index,
                                       doc_type=type,
                                       body={'ids': list_ids},
                                       _source_includes=source_includes,
                                       _source_excludes=source_excludes,
                                       _source=source)["docs"]
                for doc in docs:
                    if headless and doc["found"]:
                        yield doc.get("_source")
                    elif doc["found"]:
                        yield doc
                    elif not doc["found"]:
                        notfound_ids.add(doc["_id"])
                del list_ids[:]
        except elasticsearch.exceptions.NotFoundError:
            eprint("notfound")
            notfound_ids.add(list_ids[:chunksize])
        else:
            os.remove(idfile)
        finally:
            list_ids += list(notfound_ids)
            with open(idfile, "w") as outp:
                for _id in list_ids:
                    print(_id, file=outp)


def litter(lst, elm):
    '''
    this function produces uniq lists, and appends/inserts new elements
    lst can be a given str, list or dict, or even None
    litter() inserts elements into the given list without producing dublettes
    or makes a new lists out of the already existing objects and the inserting
    lists/objects/etc, and always checks for dublettes
    '''
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
            if isinstance(lst, (dict, str)):
                lst = [lst]
            if isinstance(lst, list):
                for element in elm:
                    if element not in lst:
                        lst.append(element)
            return lst
        else:
            return lst


def run():
    parser = argparse.ArgumentParser(description='Elasticsearch to JSON')
    parser.add_argument('-host', type=str, default="127.0.0.1",
                        help='hostname or ip of the ElasticSearch-node to use,'
                        ' default: localhost.')
    parser.add_argument('-port', type=int, default=9200,
                        help='Port of the ElasticSearch-node to use, default: 9200.')
    parser.add_argument('-index', type=str,
                        help='ElasticSearch Search Index to use')
    parser.add_argument('-type', type=str,
                        help='ElasticSearch Search Index Type to use')
    parser.add_argument('-source', type=str, help='just return this field(s)')
    parser.add_argument("-includes", type=str,
                        help="include following _source field(s)")
    parser.add_argument("-excludes", type=str,
                        help="exclude following _source field(s)")
    parser.add_argument(
        "-id", type=str, help="retrieve single document (optional)")
    parser.add_argument("-headless", action="store_true",
                        default=False, help="don't print Elasticsearch metadata")
    parser.add_argument('-body', type=json.loads, help='Searchbody')
    # no, i don't steal the syntax from esbulk...
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id?pretty. "
        "overwrites host/port/index/id/pretty")
    parser.add_argument(
        '-idfile', type=str, help="path to a file with \\n-delimited IDs to process")
    parser.add_argument('-idfile_consume', type=str,
                        help="path to a file with \\n-delimited IDs to process")
    parser.add_argument('-pretty', action="store_true",
                        default=False, help="prettyprint")
    parser.add_argument('-chunksize', type=int, default=1000,
                        help="chunksize of the search window to use")
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
        for json_record in esidfilegenerator(host=args.host,
                                             port=args.port,
                                             index=args.index,
                                             type=args.type,
                                             body=args.body,
                                             source=args.source,
                                             headless=args.headless,
                                             source_excludes=args.excludes,
                                             source_includes=args.includes,
                                             idfile=args.idfile,
                                             chunksize=args.chunksize):
            print(json.dumps(json_record, indent=tabbing))
    elif args.idfile_consume:
        for json_record in esidfileconsumegenerator(host=args.host,
                                                    port=args.port,
                                                    index=args.index,
                                                    type=args.type,
                                                    body=args.body,
                                                    source=args.source,
                                                    headless=args.headless,
                                                    source_excludes=args.excludes,
                                                    source_includes=args.includes,
                                                    idfile=args.idfile_consume,
                                                    chunksize=args.chunksize):
            print(json.dumps(json_record, indent=tabbing))
    elif not args.id:
        for json_record in esgenerator(host=args.host,
                                       port=args.port,
                                       index=args.index,
                                       type=args.type,
                                       body=args.body,
                                       source=args.source,
                                       headless=args.headless,
                                       source_excludes=args.excludes,
                                       source_includes=args.includes,
                                       verbose=True,
                                       chunksize=args.chunksize):
            print(json.dumps(json_record, indent=tabbing))
    else:
        es = elasticsearch.Elasticsearch(
            [{
                'host': args.host,
                'port': args.port,
                'max_retries': 10,
                'retry_on_timeout': True,
                'http_compress': True
            }]
        )
        json_record = ES_wrapper.call(es,
                                      'get',
                                      index=args.index,
                                      doc_type=args.type,
                                      _source=True,
                                      _source_excludes=args.excludes,
                                      _source_includes=args.includes,
                                      id=args.id)
        if json_record and args.headless:
            print(json.dumps(json_record["_source"], indent=tabbing))
        elif json_record and not args.headless:
            print(json.dumps(json_record, indent=tabbing))


if __name__ == "__main__":
    run()
