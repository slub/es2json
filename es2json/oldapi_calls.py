from es2json import ESGenerator
from es2json import IDFile
from es2json import IDFileConsume

"""
wrapper functions for deprecated es2json API calls
"""


def esidfileconsumegenerator(**kwargs):
    with IDFileConsume(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esidfilegenerator(**kwargs):
    with IDFile(**kwargs) as generator:
        for record in generator.generator():
            yield record


def esgenerator(**kwargs):
    for item in ("includes", "excludes"):
        if kwargs.get("source_{}".format(item)):
            kwargs[item] = kwargs.pop("source_{}".format(item))
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
