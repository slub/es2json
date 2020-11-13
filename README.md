# es2jon

es2json is a simple elasticsearch index download/search tool. You can use your own queries via the -body switch or give it an idfile with \n-delmited IDs. The idfile_consume switch consumes the idfile, leaving back in the file just the IDs which couldnt get retrieved because of any reasons. Output is in line-delimited JSON over STDOUT, if you don't use -headless, elasticsearch metadata is getting printed out too.

## usage 
usage: es2json [-h] [-host HOST] [-port PORT] [-index INDEX] [-type TYPE]
               [-source [SOURCE]] [-size SIZE] [-timeout TIMEOUT]
               [-includes INCLUDES] [-excludes EXCLUDES] [-id ID] [-headless]
               [-body BODY] [-server SERVER] [-idfile IDFILE]
               [-idfile_consume IDFILE_CONSUME] [-pretty] [-verbose]
               [-chunksize CHUNKSIZE]

Elasticsearch to JSON

optional arguments:
  - h, --help            show this help message and exit
  - host HOST            hostname or ip of the ElasticSearch-node to use,
                        default: localhost.
  - port PORT            Port of the ElasticSearch-node to use, default: 9200.
  - index INDEX          ElasticSearch Search Index to use
  - type TYPE            ElasticSearch Search Index Type to use
  - source [SOURCE]      return the Document or just the Elasticsearch-Metadata
  - size SIZE            just return the first n-Records of the search, or
                        return a python slice, e.g. 2:10 returns a list from
                        the 2nd including the 9th element of the search only
                        works with the ESGenerator
  - timeout TIMEOUT      Set the time in seconds after when a ReadTimeoutError
                        can occur. Default is 10 seconds. Raise for
                        big/difficult querys
  - includes INCLUDES    just include following _source field(s) in the _source
                        object
  - excludes EXCLUDES    exclude following _source field(s) from the _source
                        object
  - id ID                retrieve single document (optional)
  - headless             don't print Elasticsearch metadata
  - body BODY            Searchbody
  - server SERVER        use http://host:port/index/type/id?pretty. overwrites
                        host/port/index/id/pretty
  - idfile IDFILE        path to a file with \n-delimited IDs to process
  - idfile_consume IDFILE_CONSUME
                        path to a file with \n-delimited IDs to process
  - pretty               prettyprint
  - verbose              print progress for large dumps
  - chunksize CHUNKSIZE  chunksize of the search window to use

## tests
This package comes with tests, of course this needs to be setup. See tests/Readme for setting this up.
Running tests after setup is as easy as `python3 -m pytest tests`

