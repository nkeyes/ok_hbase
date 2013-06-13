# ok_hbase

Welcome HBase cowboys.

Read the [pages site](http://okcwest.github.io/ok_hbase/)!

## Installation

Add this line to your application's Gemfile:

    gem 'ok_hbase'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ok_hbase

## Usage

```ruby
# simple example showing how to:
# 1) connect
# 2) create a table
# 3) write rows
# 4) scan for rows
# 5) get a row by key
# 6) delete a table

require 'ok_hbase'

# get a connection
conn = OkHbase::Connection.new(
  host: 'localhost',
  port: 9090,
  auto_connect: true
)

# create a new table with column family 'd'
table = conn.create_table('ok_hbase_test', d: {})

# put a bunch of data in the table
('hbaaa'..'hbzzz').each_with_index do |row_key, index|
  table.put(
    row_key,
    {
      'd:row_number' => "#{index+1}",
      'd:message' => "this is row number #{index+1}"
    }
  )
print "wrote row: #{row_key}\r"
end

# scan for all rows beginning with 'hba'
table.scan(row_prefix: 'hba')

# get the row with the row key 'hbase'
table.row('hbase')

# clean up
conn.delete_table('ok_hbase_test', true)
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
