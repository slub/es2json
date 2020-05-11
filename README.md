# es2jon

es2json is a simple elasticsearch index download/search tool. You can use your own queries via the -body switch or give it an idfile with \n-delmited IDs. The idfile_consume switch consumes the idfile, leaving back in the file just the IDs which couldnt get retrieved because of any reasons. Output is in line-delimited JSON over STDOUT, if you don't use -headless, elasticsearch metadata is getting printed out too.

## usage
arguments:
  - h, --help            show this help message and exit
  - host HOST            hostname or ip of the ElasticSearch-node to use, default: localhost.
  - port PORT            Port of the ElasticSearch-node to use, default is 9200.
  - index INDEX          ElasticSearch Search Index to use
  - type TYPE            ElasticSearch Search Index Type to use
  - source SOURCE        just return this field(s)
  - includes INCLUDES    include following _source field(s)
  - excludes EXCLUDES    exclude following _source field(s)
  - id ID                retrieve single document (optional)
  - headless             don't include Elasticsearch Metafields
  - body BODY            Searchbody
  - server SERVER        use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty
  - idfile IDFILE        path to a file with newline-delimited IDs to process
  - idfile_consume IDFILE_CONSUME path to a file with newline-delimited IDs to process
  - pretty               prettyprint
  - chunksize CHUNKSIZE  chunksize of the search window to use

## tests
This package comes with tests, of course this needs to be setup. See tests/Readme for setting this up.
Running tests after setup is as easy as `python3 -m pytest tests`
