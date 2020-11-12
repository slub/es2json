import es2json
import json
import uuid
import os
import gzip
from copy import deepcopy

default_kwargs = {
    "host": "localhost",
    "port": 9200,
    "index": "test",
    "type": "_doc",
}

MAX = 1000

default_returnrecord = {"sort": None,
                        "_index": "test",
                        "_score": None,
                        "_type": "_doc",
                        "_source": {}
                        }


testdata = []
with gzip.open("tests/testdata.ldj.gz", "rt") as inp:
    for line in inp:
        testdata.append(json.loads(line))


def test_esgenerator(**kwargs):
    expected_records = []
    for n, record in enumerate(testdata):
        retrecord = deepcopy(default_returnrecord)
        retrecord["_source"] = record
        retrecord["_id"] = str(n)
        retrecord['sort'] = [n]
        expected_records.append(dict(sorted(retrecord.items())))
    records = []
    with es2json.ESGenerator(**default_kwargs, **kwargs) as es:
        for n, record in enumerate(es.generator()):
            records.append(dict(sorted(record.items())))

    assert sorted(expected_records, key=lambda k: k["_id"]) == sorted(records, key=lambda k: k["_id"])


def test_esgenerator_NoneSource():
    expected_records = []
    for n, record in enumerate(testdata):
        retrecord = deepcopy(default_returnrecord)
        retrecord["_id"] = str(n)
        retrecord['sort'] = [n]
        expected_records.append(dict(sorted(retrecord.items())))
    records = []
    with es2json.ESGenerator(source=False, **default_kwargs) as es:
        for record in es.generator():
            records.append(dict(sorted(record.items())))
    assert sorted(expected_records, key=lambda k: k["_id"]) == sorted(records, key=lambda k: k["_id"])


def test_esgenerator_source_includes():
    includes = ["foo"]
    expected_records = []
    for n, record in enumerate(testdata):
        retrecord = deepcopy(default_returnrecord)
        for item in includes:
            retrecord["_source"][item] = record[item]
        retrecord["_id"] = str(n)
        retrecord['sort'] = [n]
        expected_records.append(dict(sorted(retrecord.items())))
    records = []
    with es2json.ESGenerator(includes=includes, **default_kwargs) as es:
        for record in es.generator():
            records.append(dict(sorted(record.items())))
    assert sorted(expected_records, key=lambda k: k["_id"]) == sorted(records, key=lambda k: k["_id"])


def test_esgenerator_source_excludes():
    expected_records = []
    for n in range(0, MAX):
        retrecord = deepcopy(default_returnrecord)
        retrecord["_id"] = str(n)
        retrecord["_source"]["foo"] = n
        retrecord["_source"]["baz"] = "test{}".format(n)
        retrecord['sort'] = [n]
        expected_records.append(dict(sorted(retrecord.items())))
    records = []
    with es2json.ESGenerator(excludes="bar", **default_kwargs) as es:
        for record in es.generator():
            records.append(dict(sorted(record.items())))
    assert sorted(expected_records, key=lambda k: k["_id"]) == sorted(records, key=lambda k: k["_id"])


def test_esgenerator_query_headless():
    query = {"query": {"match": {"baz.keyword": "test666"}}}
    with es2json.ESGenerator(body=query, headless=True, **default_kwargs) as es:
        for record in es.generator():
            assert record == {"foo": 666, "bar": MAX-666, "baz": "test666"}


def test_esidfilegenerator_iterable():
    expected_records = []
    ids = []
    for n in range(200, 300):
        retrecord = {}
        retrecord["foo"] = n
        retrecord["baz"] = "test{}".format(n)
        retrecord["bar"] = MAX-n
        expected_records.append(dict(sorted(retrecord.items())))
        ids.append(str(n))
    records = []
    with es2json.IDFile(idfile=ids, headless=True, **default_kwargs) as es:
        for record in es.generator():
            records.append(dict(sorted(record.items())))
    assert sorted(expected_records, key=lambda k: k["foo"]) == sorted(records, key=lambda k: k["foo"])


def test_esidfilegenerator_file():
    fd = str(uuid.uuid4())
    expected_records = []
    with open(fd, "w") as outp:
        for n in range(200, 300):
            retrecord = {}
            retrecord["foo"] = n
            retrecord["baz"] = "test{}".format(n)
            retrecord["bar"] = MAX-n
            expected_records.append(dict(sorted(retrecord.items())))
            print(n, file=outp)
    records = []
    with es2json.IDFile(idfile=fd, headless=True, **default_kwargs) as es:
        for record in es.generator():
            records.append(dict(sorted(record.items())))
    os.remove(fd)
    assert sorted(expected_records, key=lambda k: k["foo"]) == sorted(records, key=lambda k: k["foo"])


def test_eidfileconsumegenerator():
    fd = str(uuid.uuid4())
    expected_records = []
    with open(fd, "w") as outp:
        for n in range(200, 300):
            retrecord = {}
            retrecord["foo"] = n
            retrecord["baz"] = "test{}".format(n)
            retrecord["bar"] = MAX-n
            expected_records.append(dict(sorted(retrecord.items())))
            print(n, file=outp)
    records = []
    with es2json.IDFileConsume(idfile=fd, headless=True, **default_kwargs) as es:
        for record in es.generator():
            records.append(dict(sorted(record.items())))
    assert sorted(expected_records, key=lambda k: k["foo"]) == sorted(records, key=lambda k: k["foo"])
    assert es2json.isfile(fd) is False


def test_eidfileconsumegenerator_missing_ids():
    fd = str(uuid.uuid4())
    expected_records = []
    found_ids = set()
    with open(fd, "w") as outp:
        for n in range(MAX-100, MAX+200):
            print(n, file=outp)
            if n < MAX:
                retrecord = {}
                retrecord["foo"] = n
                retrecord["baz"] = "test{}".format(n)
                retrecord["bar"] = MAX-n
                expected_records.append(dict(sorted(retrecord.items())))
    with es2json.IDFileConsume(idfile=fd, headless=True, **default_kwargs) as es:
        for record in es.generator():
            found_ids.add(record["foo"])
            assert dict(sorted(record.items())) in expected_records
    with open(fd, "r") as inp:
        for ppn in inp:
            assert ppn.rstrip() not in found_ids
    os.remove(fd)


def test_esfatgenerator():
    expected_records = []
    for n in range(0, MAX):
        retrecord = deepcopy(default_returnrecord)
        retrecord["_id"] = str(n)
        retrecord["_source"]["foo"] = n
        retrecord["_source"]["bar"] = MAX-n
        retrecord["_source"]["baz"] = "test{}".format(n)
        expected_records.append(dict(sorted(retrecord.items())))
    records = []
    for fatrecords in es2json.esfatgenerator(**default_kwargs):
        for record in fatrecords:
            records.append(dict(sorted(record.items())))
    assert sorted(expected_records, key=lambda k: k["_id"]) == sorted(records, key=lambda k: k["_id"])
