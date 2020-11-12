#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import elasticsearch
import elasticsearch_dsl
import argparse
import logging
import traceback
from helperscripts import *

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
                 verbose=True,
                 size=None):
        self.es = elasticsearch_dsl.connections.create_connection(**{
                'host': host,
                'port': port,
                'timeout': timeout,
                'max_retries': 10,
                'retry_on_timeout': True,
                'http_compress': True
        })
        self.source = source
        self.chunksize = chunksize
        self.headless = headless
        self.index = index
        self.doc_type = type
        self.source_excludes = excludes
        self.source_includes = includes
        self.body = body
        self.verbose = verbose
        self.size = size

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
        s = elasticsearch_dsl.Search(using=self.es, index=self.index, doc_type=self.doc_type).source(excludes=self.source_excludes, includes=self.source_includes)
        if self.body:
            s = s.update_from_dict(self.body)
        if self.verbose:
            hits_total = s.count()
        if self.size:
            if ':' in self.size:
                searchslice = slice(int(self.size.split(':')[0]), int(self.size.split(':')[1]), 1)
            else:
                searchslice = slice(0, int(self.size), 1)
            hits = s[searchslice].execute()
        else:
            hits = s.params(scroll='12h').scan()
        for n, hit in enumerate(hits):
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
    missing = None
    ids = None

    def __init__(self,  idfile=None, **kwargs):
        super().__init__(**kwargs)
        self.idfile = idfile  # string containing the path to the idfile, or an iterable containing all the IDs
        self.missing = []  # an iterable containing all the IDs which we didn't find
        self.ids = []  # an iterable containing all the IDs from idfile, going to be reduced during runtime

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
        for item in self.missing:
            eprint("ID {} not found".format(item))

    def __enter__(self):
        self.read_file()
        return self

    def __exit__(self, type, value, traceback):
        pass

    def generator(self):
        while len(self.ids) > 0:
            if self.body:
                for _id in self.ids[:self.chunksize]:
                    s = elasticsearch_dsl.Search(using=self.es, index=self.index, doc_type=self.doc_type).source(excludes=self.source_excludes, includes=self.source_includes).from_dict(self.body).query("match",_id=_id)
                    if s.count() > 0:  # we got our document
                        for n, hit in enumerate(s.execute()):
                            doc = self.return_doc(record=hit.to_dict(), hide_metadata=self.headless, meta=hit.meta.to_dict(), source=self.source)
                            if doc:
                                yield doc
                    else:  # oh no, no results, we delete it from self.ids to prevent endless loops and add it to the missing ids iterable
                        self.missing.append(_id)
                    del self.ids[self.ids.index(_id)]
            else:
                try:
                    s = elasticsearch_dsl.Document.mget(docs=self.ids[:self.chunksize],
                                                        using=self.es,
                                                        index=self.index,
                                                        _source_excludes=self.source_excludes,
                                                        _source_includes=self.source_includes,
                                                        _source=self.source,
                                                        missing='raise')
                except elasticsearch.exceptions.NotFoundError as e:
                    for doc in e.info['docs']:  # we got some missing ids and harvest the missing ids from the Elasticsearch NotFoundError Exception
                        self.missing.append(doc['_id'])
                        del self.ids[self.ids.index(doc['_id'])]
                else:  # only gets called if we don't run into an exception
                    for hit in s:
                        if hit:
                            _id = hit.meta.to_dict()["id"]
                            doc = self.return_doc(record=hit.to_dict(),
                                            hide_metadata=self.headless,
                                            meta=hit.meta.to_dict(),
                                            source=self.source)
                            yield doc
                            del self.ids[self.ids.index(_id)]
            if not self.ids:  # if we delete the last item from ids, ids turns to None and then the while(len(list())) would throw an exception, since None isn't an iterable
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
        with open(self.idfile, "w") as outp:
            if self.missing:
                for item in self.missing:
                    print(item, file=outp)
            else:  # no ids missing in the cluster? alright, we clean up
                os.remove(self.idfile)



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
    parser.add_argument('-size', type=str, default=None,
                        help='just return the first n-Records of the search,'
                        'or return a python slice, e.g. 2:10 returns a list'
                        'from the 2nd including the 9th element of the search'
                        'only works with the ESGenerator')
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
        ESGeneratorFunction = ESGenerator(**kwargs_generator).generator()
    for json_record in ESGeneratorFunction:
        print(json.dumps(json_record, indent=tabbing))


if __name__ == "__main__":
    run()
