# okhbase examples
Examples to illustrate using the api to do basic work.

## Usage

See: http://hbase.apache.org/book.html if you are new to hbase.

With bundler up to date, usage():

```bash
$ bundle exec examples/perf_read.rb -h
Usage: examples/perf_read.rb [options]
    -h, --help                       Display this help
    -v, --verbose                    Output json result
    -n, --host HOSTNAME              hostname of RegionServer or  master
    -t, --table TABLE                hbase table name
        --port PORT                  port number of thrift server, defaults to 9090
        --timeout TIMEOUT            connect timeout, defaults to 600
    -a, --array ARRAY                array values for pack, in csv, no whitespace in the format of "11111111,1,1,1,1"
    -p, --pack PACK                  template string to build binary sequence from literal passed to -a
    -i, --iterations NUM             number of iterations to run the benchmark, defaults to 10
```

the table reading examples are built around the ruby Array api using Array.pack to build binary sequences to needle our data out of the hbase haystack. Most people use keys comprised of timestamps, IDs and other data.

If we had a rowkey with a id and flags in a table called "chat" we could run the perf_read.rb example like this:

```bash
$ bundle exec examples/perf_read.rb -n 127.0.0.1 -t chat --array='22221111,7,3,4,0' -p "L>CCCC" -i 10 -v
```

## WIP
* perf_write.rb: currently a wip , use at your own risk
* table_write.rb: currently a wip, use at your own risk
* table_read.rb: tested
* perf_read.rb: tested
