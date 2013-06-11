# ok_hbase

Welcome HBase cowboys.

Read the [wiki](https://github.com/okcwest/ok-hbase/wiki)!

## Installation

Add this line to your application's Gemfile:

    gem 'ok_hbase'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ok_hbase

## Usage

```bash
$ bundle console
Resolving dependencies...
irb(main):030:0> connection = OkHbase::Connection.new(host: "hbase-dev")
=> #<OkHbase::Connection:0x00000002440140 <snip>
irb(main):031:0> table = OkHbase::Table.new(mytable, connection)
=> #<OkHbase::Table:0x00000002449f38 <snip> 
irb(main):032:0> count = 0
=> 0
irb(main):033:0> table.scan row_prefix: [ myid, 1, 5, 1, 0 ].pack("L>CCCC") do |row, col|
irb(main):034:1*   count += 1
irb(main):035:1> end
=> nil
irb(main):036:0> count
=> 1072
irb(main):037:0> connection.close
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
