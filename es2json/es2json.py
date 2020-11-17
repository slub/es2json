import os
import elasticsearch
import elasticsearch_dsl
import helperscripts


class ESGenerator:
    """
    Main generator Object where other Generators inherit from
    """
    def __init__(self, host='localhost',
                 port=9200,
                 es=None,
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
        :param es: Don't use the host/port/timeout setting, use your own elasticsearch.Elasticsearch() Object
        :param typ: Elasticsearch doc_type to use, optional, deprecated after Elasticsearch>=7.0.0
        :param body: Query body to use for Elasticsearch, optional
        :param source: Include the source field in your record, default is False
        :param excludes: don't include the fields defined by this parameter, optional, must be python list()
        :param includes: only include the fields defined by this parameter, optional, must be python list()
        :param headless: don't include the metafields, only the data in the _source field, default is False
        :param chunksize: pagesize to used, default is 1000
        :param timeout: Elasticsearch timeout parameter, default is 10 (seconds)
        :param verbose: print out progress information on /dev/stderr, default is True, optional
        :param size: only return records defined by a python slice() object
                     free earworm when working with python slices: https://youtu.be/Nlnoa67MUJU
        """
        if es:
            self.es = es
        else:
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
            return {}
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
        """
        main generator function which harvests from the Elasticsearch-Cluster after all init and argument stuff is done
        """
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
            hits = s[self.size].execute()
        else:
            hits = s.params(scroll='12h', size=self.chunksize).scan()  # in scroll context, size = pagesize, still all records will be returned
        for n, hit in enumerate(hits):
            yield self.return_doc(hit)
            if self.verbose and ((n+1) % self.chunksize == 0 or n+1 == hits_total):
                helperscripts.eprint("{}/{}".format(n+1, hits_total))


class IDFile(ESGenerator):
    """
    wrapper for esgenerator() to submit a list of ids or a file with ids
    to reduce the searchwindow on
    """
    
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
        """
        determining weather self.idfile is an iterable or a file,
        harvests the IDs out of it and saves them in a set (for de-duplication)
        """
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

    def write_file(self, missing):
        """
        writing of idfile for the consume generator,
        we instance this here to be used in generator() function, even if we
        don't use it in this parent class at this point we just like to
        error-print every missing ids
        """
        for item in missing:
            helperscripts.eprint("ID {} not found".format(item))

    def generator(self):
        """
        main generator function for IDFile and IDFileConsume
        searching with an set of IDs can take quite long time
        better would be to reduce the set of documents to a pure idlist, this is quite fast over mget
        often, its needed to do it with a search, therefore both ways work
        """
        missing = []  # an iterable containing missing ids
        while len(self.ids) > 0:
            if self.body:
                for _id in self.ids[:self.chunksize]:
                    s = elasticsearch_dsl.Search(using=self.es,
                                                 index=self.index,
                                                 doc_type=self.doc_type).source(excludes=self.source_excludes,
                                                                                includes=self.source_includes).from_dict(self.body).query("match",
                                                                                                                                          _id=_id)
                    if s.count() > 0:  # we got our document
                        for hit in s.execute():
                            yield self.return_doc(hit)
                    else:  # oh no, no results, we delete it from self.ids to prevent endless loops and add it to the missing ids iterable
                        missing.append(_id)
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
                        missing.append(doc['_id'])
                        del self.ids[self.ids.index(doc['_id'])]
                else:  # only gets called if we don't run into an exception
                    for hit in s:
                        _id = hit.meta.to_dict()["id"]
                        yield self.return_doc(hit)
                        del self.ids[self.ids.index(_id)]
            if not self.ids:
                """
                if we delete the last item from ids,
                ids turns to None and then the while(len(list()))
                would throw an exception, since None isn't an iterable
                """
                self.ids = []
        self.write_file(missing)


class IDFileConsume(IDFile):
    """
    same class like IDFile, but here we overwrite the write_file and read_file functions for missing-ID-handling purposes
    """
    def __init__(self, **kwargs):
        """
        Creates a new IDFileConsume Object
        """
        super().__init__(**kwargs)

    def read_file(self):
        """
        no more iterables here, only files
        """
        ids_set = set()
        with open(self.idfile, "r") as inp:
            for ppn in inp:
                ids_set.add(ppn.rstrip())
        self.ids = list(ids_set)

    def write_file(self, missing):
        """
        overwriting write_file so this outputs a idfile of the consume generator with the missing ids
        if no IDs are missing, that file gets deleted
        """
        if missing:
            with open(self.idfile, "w") as outp:
                for item in missing:
                    print(item, file=outp)
        else:  # no ids missing in the cluster? alright, we clean up
            os.remove(self.idfile)
