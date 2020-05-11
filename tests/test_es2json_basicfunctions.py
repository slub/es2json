import es2json
import uuid


def test_litter():
    assert es2json.litter("foo", "bar") == ["foo", "bar"]
    assert es2json.litter(["foo", "bar"], "baz") == ["foo", "bar", "baz"]
    assert es2json.litter("baz", ["foo", "bar"]) == ["baz", "foo", "bar"]
    assert es2json.litter(None, ["foo", "bar", "baz"]) == ["foo", "bar", "baz"]
    assert es2json.litter(["foo", "foobar"], ["bar", "baz"]) == ["foo", "foobar", "bar", "baz"]
    assert es2json.litter(["foo", "foobar", "bar"], ["bar", "baz"]) == ["foo", "foobar", "bar", "baz"]


def test_isint():
    assert es2json.isint("2")
    assert es2json.isint("2.5") is False
    assert es2json.isint(2)
    assert es2json.isint({"This is": "a dict"}) is False
    assert es2json.isint(["this", "is", "a", "list"]) is False


def test_isfloat():
    assert es2json.isfloat("2")
    assert es2json.isfloat("2.5")
    assert es2json.isfloat(2)
    assert es2json.isfloat({"This is": "a dict"}) is False
    assert es2json.isfloat(["this", "is", "a", "list"]) is False


def test_isiter():
    assert es2json.isiter("2")
    assert es2json.isiter("2.5")
    assert es2json.isiter(2) is False
    assert es2json.isiter({"This is": "a dict"})
    assert es2json.isiter(["this", "is", "a", "list"])


def test_isfile():
    assert es2json.isfile("tests/test_es2json_basicfunctions.py")
    assert es2json.isfile("es2json/es2json.py")
    assert es2json.isfile("tests/test_es2json.py_basicfunctions"+str(uuid.uuid4())) is False


def test_ArrayOrSingleValue():
    assert es2json.ArrayOrSingleValue(2) == 2
    assert es2json.ArrayOrSingleValue([2]) == 2
    assert es2json.ArrayOrSingleValue([1, 2]) == [1, 2]
    assert es2json.ArrayOrSingleValue("abc") == "abc"
    assert es2json.ArrayOrSingleValue(["abc"]) == "abc"
    assert es2json.ArrayOrSingleValue(["abc", "def"]) == ["abc", "def"]
    assert es2json.ArrayOrSingleValue([{"foo": "bar"}]) == {"foo": "bar"}
    assert es2json.ArrayOrSingleValue([{"foo": "bar"}, {"bar": "foo"}]) == [{"foo": "bar"}, {"bar": "foo"}]
    assert es2json.ArrayOrSingleValue({}) is None
    assert es2json.ArrayOrSingleValue([]) is None
