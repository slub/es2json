from es2json import *

"""
wrapper functions for deprecated es2json API calls
"""

"""
rewrite dict of some old named parameters, we reroute them to the new name parameters
"""
_map_args = {"type": "type_",
             "id": "id_",
             "size": "slice_",
             "source_includes": "includes",
             "source_excludes": "excludes"}


def esidfileconsumegenerator(**kwargs):
    for k, v in _map_args.items():
        if k in kwargs:
            kwargs[v] = kwargs.pop(k)
    with IDFileConsume(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esidfilegenerator(**kwargs):
    for k, v in _map_args.items():
        if k in kwargs:
            kwargs[v] = kwargs.pop(k)
    with IDFile(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esgenerator(**kwargs):
    for k, v in _map_args.items():
        if k in kwargs:
            kwargs[v] = kwargs.pop(k)
    with ESGenerator(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esfatgenerator(**kwargs):
    """
    HIGHLY DEPRECATED !!! do not use !!!
    this is an workaround function for the old esfatgenerator
    the performance-boost was an illusion
    only kept in here to not break old python tools
    """
    for k, v in _map_args.items():
        if k in kwargs:
            kwargs[v] = kwargs.pop(k)
    kwargs["headless"] = False
    if not kwargs.get("chunksize"):
        kwargs["chunksize"] = 1000
    chunks = []
    with ESGenerator(**kwargs) as generator:
        for record in generator.generator():
            chunks.append(record)
            if len(chunks) == kwargs["chunksize"]:
                yield chunks
                chunks = []
    if chunks:
        yield chunks
