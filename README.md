# es2jon

es2json is a simple elasticsearch index download/search tool. You can use your own queries via the -body switch or give it an idfile with \n-delmited IDs. The idfile\_consume switch consumes the idfile, leaving back in the file just the IDs which couldnt get retrieved because of any reasons. Output is in line-delimited JSON over STDOUT, if you don't use -headless, elasticsearch metadata is getting printed out too.

## usage

```
usage: es2json [-h] [-server SERVER] [-ign-source] [-size N[:M]]
               [-timeout TIMEOUT] [-includes INCLUDES] [-excludes EXCLUDES]
               [-headless] [-body BODY] [-idfile IDFILE]
               [-idfile_consume IDFILE_CONSUME] [-pretty] [-verbose]
               [-chunksize CHUNKSIZE] [-auth [USER]]

Query elasticsearch indices/index/documents and print them formatted as JSON-Objects

optional arguments:
  -h, --help            show this help message and exit
  -server SERVER        use http://host:port/index/type/id.
                        host:port - hostname or IP with port of the elasticsearch node to query
                                    default: localhost:9200
                        index     - index to query
                                    default: None → queries across all available indices
                        type      - elasticsearch doctype to use (optional)
                        id        - identifier of one specific document to query (optional)
  -ign-source           return the Document or just the Elasticsearch-Metadata
  -size N[:M]           just return the first n-Records of the search,
                        or return a python slice, e.g. 2:10 returns a list
                        from the 2nd including the 9th element of the search
                        only works with the ESGenerator
                        Note: Not all slice variants may be supported
  -timeout TIMEOUT      Set the time in seconds after when a ReadTimeoutError can occur.
                        Default is 10 seconds. Raise for big/difficult querys 
  -includes INCLUDES    just include following _source field(s) in the _source object
  -excludes EXCLUDES    exclude following _source field(s) from the _source object
  -headless             don't print Elasticsearch metadata
  -body BODY            Elasticsearch Query object that can be in the form of
                        1) a JSON string (e.g. '{"query": {"match": {"name": "foo"}}}')
                        2) a file containing the upper query string
  -idfile IDFILE        path to a file with \n-delimited IDs to process
  -idfile_consume IDFILE_CONSUME
                        path to a file with \n-delimited IDs to process
  -pretty               prettyprint the json output
  -verbose              print progress for large dumps
  -chunksize CHUNKSIZE  chunksize of the search window to use
  -auth [USER]          Provide authentication, this can be done using:
                        1) set environment variables E2J_USER and E2J_PASSWD. In
                           this case there is no further argument needed here
                        2) as a string "username". The password is then asked interactively
                        3) as "username:password" (not recommended)

```

## tests
This package comes with tests, of course this needs to be setup. See tests/Readme for setting this up.
Running tests after setup is as easy as `python3 -m pytest tests`

