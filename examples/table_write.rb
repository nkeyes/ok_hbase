#!/usr/bin/env ruby
#
# perf_read.rb - basic read perf test

$:.unshift File.expand_path('../../lib', __FILE__)
$stdout.sync = true

require 'awesome_print'
require 'ok_hbase'
require 'optparse'
require 'logger'

$options = {}
$logger = Logger.new(STDOUT)
$logger.formatter = proc { |severity, datetime, progname, msg| "#{datetime} #{severity}: #{msg}\n" }
$logger.level = Logger::INFO

def usage(error=nil)
  puts "Error: #{error}\n\n" if error
  puts $optparse
  exit 1
end

def get_connection
  $logger.debug 'Setting up connection'


  $logger.debug "Connecting to #{$options[:host]}"
  OkHbase::Connection.new(
      auto_connect: true,
      host: $options[:host],
      port: $options[:port],
      timeout: $options[:timeout]
  )
end

def get_table(table, conn)
  if table.nil?
    $logger.fatal 'Must specify a table'
    return nil
  end
  $logger.debug "Get instance for table #{table}"
  OkHbase::Table.new(table, conn)
end

def get_row_count(conn, prefix)
  row_count = 0
  conn.scan row_prefix: prefix, caching: 5000 do |row, cols|
    row_count += 1
  end

  row_count
end


def main()
  $optparse = OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [options]"

    $options[:host] = 'localhost'
    $options[:port] = 9090
    $options[:timeout] = 600

    opts.on('-h', '--help', 'Display this help') do
      usage
    end

    opts.on('-H', '--host HOST', "host or ip address where thrift server is running, defaults to #{$options[:host]}") do |host|
      $options[:host] = host
    end

    opts.on('-t', '--table TABLE', 'hbase table name') do |table|
      $options[:table] = table
    end

    opts.on('-p', '--port PORT', "port number of thrift server, defaults to #{$options[:port]}") do |port|
      $options[:port] = port.to_i
    end

    opts.on('--timeout TIMEOUT', "connect timeout, defaults to #{$options[:timeout]}") do |timeout|
      $options[:timeout] = timeout.to_i
    end

    opts.on('-P', '--prefix ROW_PREFIX', "row prefix to use in scan") do |prefix|
      $options[:prefix] = prefix
    end


  end

  usage "You didn't specify any options" if not ARGV[0]

  $optparse.parse!

  usage "You didn't specify a table" if not $options[:table]
  usage "You didn't specify a prefix" if not $options[:prefix]

  connection = get_connection()
  table = get_table($options[:table], connection)

  table.scan(row_prefix: $options[:prefix]) do |row_key, columns|
    ap row_key => columns
  end
end

main() if __FILE__ == $0
