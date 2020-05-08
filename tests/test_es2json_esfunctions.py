import es2json
import json
import uuid
import os

host = "localhost"
port = 9200
testindex = "test"
test_doctype = "_doc"

def test_esgenerator():
    for n, record in enumerate(es2json.esgenerator(host=host,
                                                   port=port,
                                                   index=testindex,
                                                   type=test_doctype)):
        expected = {"_index": "test",
                    "_type": "_doc",
                    "_id": str(n),
                    "_score": 1.0,
                    "_source": {
                        "foo": n,
                        "bar": 1000-n,
                        "baz": "test{}".format(n)
                        }
                    }

        assert record == expected


def test_esgenerator_NoneSource():
    for n, record in enumerate(es2json.esgenerator(host=host,
                                                   port=port,
                                                   index=testindex,
                                                   type=test_doctype,
                                                   source="None")):
        expected = {"_index": "test",
                    "_type": "_doc",
                    "_id": str(n),
                    "_score": 1.0,
                    "_source": {}
                    }
        assert record == expected


def test_esgenerator_source_includes():
    for n, record in enumerate(es2json.esgenerator(host=host,
                                                   port=port,
                                                   index=testindex,
                                                   type=test_doctype,
                                                   source_includes="foo")):
        expected = {"_index": "test",
                    "_type": "_doc",
                    "_id": str(n),
                    "_score": 1.0,
                    "_source": {
                        "foo": n
                        }
                    }

        assert record == expected


def test_esgenerator_source_excludes():
    for n, record in enumerate(es2json.esgenerator(host=host,
                                                   port=port,
                                                   index=testindex,
                                                   type=test_doctype,
                                                   source_excludes="bar")):
        expected = {"_index": "test",
                    "_type": "_doc",
                    "_id": str(n),
                    "_score": 1.0,
                    "_source": {
                        "foo": n,
                        "baz": "test{}".format(n)
                        }
                    }

        assert record == expected


def test_esgenerator_query_headless():
    query = {"query": {"match": {"baz.keyword": "test666"}}}
    expected =  {"foo": 666,
                 "bar": 1000-666,
                 "baz": "test666"
                 }

    for n, record in enumerate(es2json.esgenerator(host=host,
                                                    port=port,
                                                    index=testindex,
                                                    type=test_doctype,
                                                    body=query,
                                                    headless=True)):
        
        assert record == expected


def test_esidfilegenerator_iterable():
    ids = [str(x) for x in range(200,300)]
    expected_records = []
    records = []
    for n, record in enumerate(es2json.esidfilegenerator(host=host,
                                                         port=port,
                                                         index=testindex,
                                                         type=test_doctype,
                                                         headless=True,
                                                         idfile=ids)):
        expected_records.append({"foo": 200+n,
                                 "bar": 800-n,
                                 "baz": "test{}".format(200+n)
                                 })
        records.append(record)
    expected_sorted = sorted(expected_records, key=lambda k: k['foo'])
    records_sorted = sorted(records, key=lambda k: k['foo'])
    
    assert records_sorted == expected_sorted


def test_esidfilegenerator_file():
    ids = [str(x) for x in range(200,300)]
    fd = str(uuid.uuid4())
    with open(fd,"w") as outp:
        for _id in ids:
            print(_id,file=outp)
    expected_records = []
    records = []
    for n, record in enumerate(es2json.esidfilegenerator(host=host,
                                                         port=port,
                                                         index=testindex,
                                                         type=test_doctype,
                                                         headless=True,
                                                         idfile=fd)):
        expected_records.append({"foo": 200+n,
                                 "bar": 800-n,
                                 "baz": "test{}".format(200+n)
                                 })
        records.append(record)
    os.remove(fd)
    expected_sorted = sorted(expected_records, key=lambda k: k['foo'])
    records_sorted = sorted(records, key=lambda k: k['foo'])
    
    assert records_sorted == expected_sorted


def test_eidfileconsumegenerator():
    ids = [str(x) for x in range(200,300)]
    fd = str(uuid.uuid4())
    with open(fd,"w") as outp:
        for _id in ids:
            print(_id,file=outp)
    expected_records = []
    records = []
    for n, record in enumerate(es2json.esidfileconsumegenerator(host=host,
                                                         port=port,
                                                         index=testindex,
                                                         type=test_doctype,
                                                         headless=True,
                                                         idfile=fd)):
        expected_records.append({"foo": 200+n,
                                 "bar": 800-n,
                                 "baz": "test{}".format(200+n)
                                 })
        records.append(record)
    expected_sorted = sorted(expected_records, key=lambda k: k['foo'])
    records_sorted = sorted(records, key=lambda k: k['foo'])
    
    assert records_sorted == expected_sorted
    assert os.stat(fd).st_size == 0
    os.remove(fd)



def test_eidfileconsumegenerator_missing_ids():
    ids = [str(x) for x in range(900,1100)]
    fd = str(uuid.uuid4())
    missing_ids = set()
    found_ids = set()
    with open(fd,"w") as outp:
        for _id in ids:
            print(_id,file=outp)
    expected_records = []
    for n in range(900,1000):
        expected_records.append({"foo": n,
                                 "bar": 1000-n,
                                 "baz": "test{}".format(n)
                                 })
    for record in es2json.esidfilegenerator(host=host,
                                                         port=port,
                                                         index=testindex,
                                                         type=test_doctype,
                                                         headless=True,
                                                         idfile=fd):
        found_ids.add(record["foo"])
        assert record in expected_records
    with open(fd, "r") as inp:
        for ppn in inp:
            assert ppn.rstrip() not in found_ids
    os.remove(fd)
