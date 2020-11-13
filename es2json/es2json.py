import os
import elasticsearch
import elasticsearch_dsl
import helperscripts


class ESGenerator:
    """
    Main generator Object where other Generators inherit from
    """
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
        """
        Construct a new ESGenerator Object.
        :param host: Elasticsearch host to use, default is localhost
        :param port: Elasticsearch port to use, default is 9200
        :param index: Elasticsearch Index to use, optional, if no parameter given, ESGenerator uses ALL the indices
        :param typ: Elasticsearch doc_type to use, optional, deprecated after Elasticsearch>=7.0.0
        :param body: Query body to use for Elasticsearch, optional
        :param source: Include the source field in your record, default is False
        :param excludes: don't include the fields defined by this parameter, optional
        :param includes: only include the fields defined by this parameter, optional
        :param headless: don't include the metafields, only the data in the _source field, default is False
        :param chunksize: pagesize to used, default is 1000
        :param timeout: Elasticsearch timeout parameter, default is 10 (seconds)
        :param verbose: print out progress information on /dev/stderr, default is True, optional
        :param size: only return the first n records or defined by a python slice
        submitted by this parameter, optional
        """
        self.es = elasticsearch_dsl.connections.create_connection(**{
                'host': host,
                'port': port,
                'timeout': timeout,
                'max_retries': 10,
                'retry_on_timeout': True,
                'http_compress': True
        })
        self.id = id
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

    def return_doc(self, hit):
        """
        prints out the elasticsearch record defined by user input
        also rewrites the metadata fields back to NonPythonic Elasticsearch Standard
        see elasticsearch_dsl.utils.py::ObjectBase(AttrDict)__init__.py
        :param hit: The hit returned from the elasticsearch_dsl-call, is always
        """
        meta = hit.meta.to_dict()
        if self.headless and not self.source:
            helperscripts.eprint("ERROR! do not use -headless and -source False at the same Time!")
            exit(-1)
        for key in elasticsearch_dsl.utils.META_FIELDS:
            if key in meta:
                meta["_{}".format(key)] = meta.pop(key)
        if "doc_type" in meta:
            meta["_type"] = meta.pop("doc_type")
        meta["_source"] = {}
        if self.headless:
            return hit.to_dict()
        else:
            if self.source:
                meta["_source"] = hit.to_dict()
            return meta

    def __enter__(self):
        """
        function needed for with-statement
        __enter__ only returns the instanced object
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        function needed for with-statement
        since we don't need to do any cleanup, this function does nothing
        """
        pass

    def generator(self):
        if self.id:
            s = elasticsearch_dsl.Document.get(using=self.es,
                                               index=self.index,
                                               id=self.id,
                                               _source_excludes=self.source_excludes,
                                               _source_includes=self.source_includes,
                                               _source=self.source)
            yield self.return_doc(s)
            return
        s = elasticsearch_dsl.Search(using=self.es,
                                     index=self.index,
                                     doc_type=self.doc_type).source(excludes=self.source_excludes,
                                                                    includes=self.source_includes)
        if self.body:
            s = s.update_from_dict(self.body)
        if self.verbose:
            hits_total = s.count()
        if self.size:
            """
            we build the slice() object here, if this fails because of user input,
            the stacktrace of slice() is very informative, so we don't do our own Error handling here
            for size-searches, we don't use a scroll since the user wants only a small searchwindow
            """
            if ':' in self.size:
                searchslice = slice(int(self.size.split(':')[0]), int(self.size.split(':')[1]), 1)
            else:
                searchslice = slice(0, int(self.size), 1)
            hits = s[searchslice].execute()
        else:
            hits = s.params(scroll='12h', size=self.chunksize).scan()  # in scroll context, size = pagesize, still all records will be returned
        for n, hit in enumerate(hits):
            doc = self.return_doc(hit)
            if doc:
                yield doc
            if self.verbose and (n+1) % self.chunksize == 0 or n+1 == hits_total:
                helperscripts.eprint("{}/{}".format(n+1, hits_total))


class IDFile(ESGenerator):
    """
    wrapper for esgenerator() to submit a list of ids or a file with ids
    to reduce the searchwindow on
    """
    missing = []  # an iterable containing all the IDs which we didn't find

    def __init__(self,  idfile, **kwargs):
        """
        Creates a new IDFile Object
        :param idfile: the path of the file containing the IDs or an iterable containing the IDs
        """
        super().__init__(**kwargs)
        self.idfile = idfile  # string containing the path to the idfile, or an iterable containing all the IDs
        self.ids = []  # an iterable containing all the IDs from idfile, going to be reduced during runtime
        self.read_file()

    def read_file(self):
        ids_set = set()
        if isinstance(self.idfile, str) and helperscripts.isfile(self.idfile):
            with open(self.idfile, "r") as inp:
                for ppn in inp:
                    ids_set.add(ppn.rstrip())
        elif helperscripts.isiter(self.idfile) and not isinstance(self.idfile, str) and not helperscripts.isfile(self.idfile):
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
        error-print every missing ids
        """
        for item in self.missing:
            helperscripts.eprint("ID {} not found".format(item))

    def generator(self):
        while len(self.ids) > 0:
            if self.body:
                for _id in self.ids[:self.chunksize]:
                    s = elasticsearch_dsl.Search(using=self.es,
                                                 index=self.index,
                                                 doc_type=self.doc_type).source(excludes=self.source_excludes,
                                                                                includes=self.source_includes).from_dict(self.body).query("match",
                                                                                                                                          _id=_id)
                    if s.count() > 0:  # we got our document
                        for n, hit in enumerate(s.execute()):
                            doc = self.return_doc(hit)
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
                            doc = self.return_doc(hit)
                            yield doc
                            del self.ids[self.ids.index(_id)]
            if not self.ids:
                """
                if we delete the last item from ids,
                ids turns to None and then the while(len(list()))
                would throw an exception, since None isn't an iterable
                """
                self.ids = []
        self.write_file()


class IDFileConsume(IDFile):
    """
    same class like IDFile, but here we overwrite the write_file and read_file functions to just use files, no iterables anymore
    """
    def __init__(self, **kwargs):
        """
        Creates a new IDFileConsume Object
        """
        super().__init__(**kwargs)

    def read_file(self):
        ids_set = set()
        with open(self.idfile, "r") as inp:
            for ppn in inp:
                ids_set.add(ppn.rstrip())
        self.ids = list(ids_set)

    def write_file(self):
        """
        overwriting __exit so this outputs a idfile of the consume generator with the missing ids (or none)
        """
        with open(self.idfile, "w") as outp:
            if self.missing:
                for item in self.missing:
                    print(item, file=outp)
            else:  # no ids missing in the cluster? alright, we clean up
                os.remove(self.idfile)
