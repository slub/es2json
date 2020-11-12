#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import elasticsearch
import elasticsearch_dsl
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
        if server_version >= 7 and "doc_type" in kwargs and action in ("mget", "search"):  # doc_type obsolete after Major Version 7 for search and mget
            kwargs.pop("doc_type")
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


class ESGenerator:
    """
    Main generator Object where other Generators inherit from
    """
    es = None
    source = False
    chunksize = None
    headless = False
    index = None
    doc_type = None
    id = None
    body = None
    source_excludes = None
    source_includes = None
    verbose = True
    progress = 0
    size = None

    def __init__(self, host=None,
                 port=9200,
                 index=None,
                 type=None,
                 id=None,
                 body=None,
                 source=True,
                 excludes=None,
                 includes=None,
                 headless=False,
                 chunksize=1000,
                 timeout=10,
                 verbose=True):
        self.es = elasticsearch.Elasticsearch(
            [{
            'host': host,
            'port': port,
            'timeout': timeout,
            'max_retries': 10,
            'retry_on_timeout': True,
            'http_compress': True
            }]
            )
        self.source = source
        self.chunksize = chunksize
        self.headless = headless
        self.index = index
        self.doc_type = type
        self.source_excludes = excludes
        self.source_includes = includes
        self.body = body
        self.verbose = verbose

    def return_doc(self, record, hide_metadata, meta, source):
        """
        prints out the elasticsearch record defined by user input
        if source is False, only metadata fields are printed out
        if headless is true, metadata also gets printed out
        also rewrites the metadata fields back to NonPythonic Elasticsearch Standard
        see elasticsearch_dsl.utils.py::ObjectBase(AttrDict)__init__.py
        """
        if hide_metadata and not source:
            eprint("ERROR! do not use -headless and -source False at the same Time!")
            exit(-1)
        for key in elasticsearch_dsl.utils.META_FIELDS:
            if key in meta:
                meta["_{}".format(key)] = meta.pop(key)
        if "doc_type" in meta:
            meta["_type"] = meta.pop("doc_type")
        meta["_source"] = {}
        if hide_metadata:
            return record
        else:
            if source:
                meta["_source"] = record
            return meta

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def generator(self):
        if self.id:
            s = elasticsearch_dsl.Document.get(using=self.es, index=self.index, id=self.id, _source_excludes=self.source_excludes, _source_includes=self.source_includes, _source=self.source)
            doc = self.return_doc(record=s.to_dict(), hide_metadata=self.headless, meta=s.meta.to_dict(), source=self.source)
            if doc:
                yield doc
            return
        s = elasticsearch_dsl.Search(using=self.es, index=self.index, doc_type=type).source(excludes=self.source_excludes, includes=self.source_includes)
        if self.body:
            s = s.update_from_dict(self.body)
        if self.verbose:
            hits_total = s.count()
        search = s.params(scroll='12h').scan()
        for n, hit in enumerate(search):
            doc = self.return_doc(record=hit.to_dict(), hide_metadata=self.headless, meta=hit.meta.to_dict(), source=self.source)
            if doc:
                yield doc
            if self.verbose and (n+1)%self.chunksize==0 or n+1==hits_total:
                eprint("{}/{}".format(n+1, hits_total))


class IDFile(ESGenerator):
    """
    wrapper for esgenerator() to submit a list of ids or a file with ids
    to reduce the searchwindow on
    """
    iterable = None
    idfile = None
    ids = None

    def __init__(self,  idfile=None, **kwargs):
        super().__init__(**kwargs)
        self.idfile = idfile  # string containing the path to the idfile, or an iterable containing all the IDs
        self.iterable = []  # an iterable containing all the IDs from idfile, not going to be reduced during runtime
        self.ids = []  # an iterable containing all the IDs from idfile, going to be reduced during runtime and for checks at the end if everything was found

    def read_file(self):
        ids_set = set()
        if isinstance(self.idfile, str) and isfile(self.idfile):
            with open(self.idfile, "r") as inp:
                for ppn in inp:
                    ids_set.add(ppn.rstrip())
        elif isiter(self.idfile) and not isinstance(self.idfile, str) and not isfile(self.idfile):
            for ppn in self.idfile:
                ids_set.add(ppn.rstrip())
        else:
            raise AttributeError
        self.iterable = list(ids_set)
        self.ids = list(ids_set)

    def write_file(self):
        """
        writing of idfile for the consume generator,
        we instance this here to be used in generator() function, even if we
        don't use it in this parent class at this point we just like to
        error-print every non missing ids
        """
        missing = list()
        for item in self.ids:
            if item not in self.iterable:
                eprint("ID {} not found".format(item))

    def __enter__(self):
        self.read_file()
        return self

    def __exit__(self, type, value, traceback):
        pass

    def generator(self):
        while len(self.ids) > 0:
            if self.body:
                for n,_id in enumerate(self.ids[:self.chunksize]):
                    searchbody = elasticsearch_dsl.Search.from_dict(self.body).query("match", id=_id)
                    with ESGenerator(host=self.host,
                                     port=self.port,
                                     index=self.index,
                                     type=self.doc_type,
                                     body=searchbody.to_dict(),
                                     source=self.source,
                                     excludes=self.source_excludes,
                                     includes=self.source_includes,
                                     headless=False,
                                     timeout=self.timeout,
                                     verbose=False) as generatorObject:
                        for hit in generatorObject:
                            if hit:
                                doc = self.return_doc(record=hit.to_dict(),
                                         hide_metadata=self.headless,
                                         meta=hit.meta.to_dict(),
                                         source=self.source)
                                yield doc
                                del self.ids[n]
            else:
                s = elasticsearch_dsl.Document.mget(docs=self.ids[:self.chunksize],
                                                    using=self.es,
                                                    index=self.index,
                                                    _source_excludes=self.source_excludes,
                                                    _source_includes=self.source_includes,
                                                    _source=self.source)
                for hit in s:
                    if hit:
                        _id = hit.meta.to_dict()["id"]
                        doc = self.return_doc(record=hit.to_dict(),
                                         hide_metadata=self.headless,
                                         meta=hit.meta.to_dict(),
                                         source=self.source)
                        yield doc
                        del self.ids[self.ids.index(_id)]
            if not self.ids:
                self.ids = []
        self.write_file()


class IDFileConsume(IDFile):
    """
    same class like IDFile, but here we use the write_idfile function
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def read_file(self):
        ids_set = set()
        with open(self.idfile, "r") as inp:
            for ppn in inp:
                ids_set.add(ppn.rstrip())
        self.iterable = list(ids_set)
        self.ids = list(ids_set)

    def write_file(self):
        """
        overwriting __exit so this outputs a idfile f the consume generator with the missing ids (or none),
        we instance this here to be used in generator() function, even if we don't use it in this parent class
        at this point we just like to error-print every non missing ids
        """
        missing = list()
        with open(self.idfile, "w") as outp:
            if self.ids:
                for item in self.ids:
                    if item not in self.iterable:
                        print(item, file=outp)
            else:  # no ids missing in the cluster? alright, we clean up
                os.remove(self.idfile)


def esidfileconsumegenerator(**kwargs):
    """
    wrapper function for deprecated es2json API calls
    """
    with IDFileConsume(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esidfilegenerator(**kwargs):
    """
    wrapper function for deprecated es2json API calls
    """
    with IDFile(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esgenerator(**kwargs):
    """
    wrapper function for deprecated es2json API calls
    """
    for item in ("includes", "excludes"):
        if kwargs.get("source_{}".format(item)):
            kwargs[item] = kwargs.pop("source_{}".format(item))
    with ESGenerator(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esfatgenerator(**kwargs):
    """
    HIGHLY DEPRECATED !!! do not use !!! only kept in here to not break old python tools
    workaround wrapper function for deprecated es2json API calls
    """
    kwargs["headless"] = False
    if not kwargs.get("chunksize"):
        kwargs["chunksize"] = 1000
    chunks = []
    with ESGenerator(**kwargs) as generator:
        for record in enumerate(generator.generator()):
            chunks.append(record)
            if len(chunks) == kwargs["chunksize"]:
                yield chunks
                chunks = []
    if chunks:
        yield chunks


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


def str2bool(v):
    """
    https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
    """
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1', "none"):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


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
    parser.add_argument('-source', type=str2bool, nargs='?',
                        const=True, default=True,
                        help='return the Document or just the Elasticsearch-Metadata')
    parser.add_argument('-size', type=int, default=None,
                        help='just return the first n-Records of the search')
    parser.add_argument('-fat', type=str2bool, nargs='?',
                        const=True, default=True,
                        help='use the fatgenerator to get whole elasticsearch pagechunks')
    parser.add_argument("-includes", type=str,
                        help="just include following _source field(s) in the _source object")
    parser.add_argument("-excludes", type=str,
                        help="exclude following _source field(s) from the _source object")
    parser.add_argument(
        "-id", type=str, help="retrieve single document (optional)")
    parser.add_argument("-headless", type=str2bool, nargs='?',
                        const=True, default=True, help="don't print Elasticsearch metadata")
    parser.add_argument('-body', type=json.loads, help='Searchbody')
    # no, i don't steal the syntax from esbulk...
    parser.add_argument('-server', type=str, help="use http://host:port/index/type/id?pretty. "
        "overwrites host/port/index/id/pretty")
    parser.add_argument(
        '-idfile', type=str, help="path to a file with \\n-delimited IDs to process")
    parser.add_argument('-idfile_consume', type=str,
                        help="path to a file with \\n-delimited IDs to process")
    parser.add_argument('-pretty', type=str2bool, nargs='?',
                        const=True, default=False, help="prettyprint")
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
    kwargs_generator = dict(**vars(args))
    kwargs_generator.pop("server")
    kwargs_generator.pop("pretty")
    kwargs_generator.pop("fat")
    if args.idfile:
        ESGeneratorFunction = IDFile(**kwargs_generator).generator()
    elif args.idfile_consume:
        kwargs_generator["idfile"] = kwargs_generator.pop("idfile_consume")
        ESGeneratorFunction = IDFileConsume(**kwargs_generator).generator()
    else:
        kwargs_generator.pop("idfile")
        kwargs_generator.pop("idfile_consume")
        if args.fat:
            ESGeneratorFunction = esfatgenerator(**kwargs_generator)
        else:
            ESGeneratorFunction = ESGenerator(**kwargs_generator).generator()
    for json_record in ESGeneratorFunction:
        print(json.dumps(json_record, indent=tabbing))


if __name__ == "__main__":
    run()
