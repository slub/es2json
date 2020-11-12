import elasticsearch
import json
import sys
import os
from httplib2 import Http  # needed for put_dict
from argparse import ArgumentTypeError


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
        raise ArgumentTypeError('Boolean value expected.')


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
