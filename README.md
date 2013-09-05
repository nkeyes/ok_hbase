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

puts 'putting a bunch of data in the table.'
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

puts "\nscanning for all rows, starting with 'hbzza'"
table.scan(start_row: 'hbzza') do |row_key, columns|
  puts row_key, columns, "\n"
end

puts "\nscanning for all rows, stopping with 'hbaaz'"
# stop_row is NOT inclusive, so use the next greatest value
table.scan(stop_row: 'hbab') do |row_key, columns|
  puts row_key, columns, "\n"
end

puts "\nscanning for the row with keys 'hbase' and 'hbasf'"
# stop_row is NOT inclusive, so use the next greatest value
table.scan(start_row: 'hbase', stop_row: 'hbasg') do |row_key, columns|
  puts row_key, columns, "\n"
end

puts "\nscanning for all rows with keys beginning with 'hba'"
table.scan(row_prefix: 'hba')do |row_key, columns|
  puts row_key, columns, "\n"
end

puts "\ngetting the row with the row key 'hbase'"
puts table.row('hbase')

puts "\ncleaning up"
conn.delete_table('ok_hbase_test', true)
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
