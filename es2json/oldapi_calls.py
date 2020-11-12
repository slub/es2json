from es2json import *


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
        for record in generator.generator():
            chunks.append(record)
            if len(chunks) == kwargs["chunksize"]:
                yield chunks
                chunks = []
    if chunks:
        yield chunks
