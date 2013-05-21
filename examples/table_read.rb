#!/usr/bin/env /usr/bin/ruby
#
# table_read.rb - basic table read examples

$:.unshift File.expand_path('../../lib', __FILE__)
$stdout.sync = true

require 'awesome_print'
require 'ok_hbase'
require 'optparse'
require 'logger'

$options = {}
$logger = Logger.new(STDOUT)
# $logger.formatter = proc do |severity, date, progname, msg| "#{date} #{severity}: #{msg}\n" end
$logger.formatter = proc { |severity, datetime, progname, msg| "#{datetime} #{severity}: #{msg}\n" }
$logger.level = Logger::FATAL

def usage(error=nil)
  puts "Error: #{error}\n\n" if error
  puts $optparse
  exit 1
end

def get_connection(table=nil)
  $logger.debug "Setting up connection for table #{table}"
  if table.nil?
    $logger.fatal "Must specify a table"
    return nil
  end
    
  $logger.debug "Connecting to #{$options[:hostname]}"
  conn = OkHbase::Connection.new(auto_connect: true, host: $options[:hostname], port: $options[:port],
      timeout: $options[:timeout])
  $logger.debug "Get instance for table #{table}"
  OkHbase::Table.new(table, conn)
end

def get_filter()
  $logger.debug "Setup byte sequence using #{$options[:filter_array].to_s}.pack(#{$options[:filter_pack].to_s})"
  $options[:filter_array].pack($options[:filter_pack].to_s)
end

def get_row_count(conn, prefix)
  row_count = 0
  $logger.debug "Getting row count"
  conn.scan row_prefix: prefix, caching: 5000 do |row, cols|
    $logger.debug row.to_s
    row_count += 1
  end

  row_count
end

def main()
  optparse = OptionParser.new do|opts|
    opts.banner = "Usage: #{__FILE__} [options]"

    $options[:verbose] = false
    $options[:port] = 9090
    $options[:timeout] = 600

    opts.on('-h', '--help', 'Display this help') do
      usage
    end

    opts.on('-v', '--verbose', 'Output json result') do
      $options[:verbose] = true
      $logger.level = Logger::DEBUG
    end
   
    opts.on('-n', '--host HOSTNAME', 'hostname of RegionServer or  master') do |hostname|
      $options[:hostname] = hostname
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

    opts.on('-a', '--array ARRAY', Array, "array values for pack, in csv, no whitespace in the format of \"11111111,1,1,1,1\"") do |ar|
      $options[:filter_array] = ar.map(&:to_i)
    end

    opts.on('-p', '--pack PACK', "template string to build binary sequence from literal passed to -a") do |pack|
      $options[:filter_pack] = pack.to_s
    end

  end

  usage "You didn't specify any options" if not ARGV[0]

  optparse.parse!

  usage "You didn't specify a hostname" if not $options[:hostname]
  usage "You didn't specify a table" if not $options[:table]
  usage "You didn't specify an array literal" if not $options[:filter_array]
  usage "You didn't specify a binary sequence template" if not $options[:filter_pack]

  start_time = Time.now
  c = get_connection($options[:table])
  filter = get_filter()
  count = get_row_count(c, filter).to_s
  total_time = Time.now - start_time
  puts "Found #{count} row(s) in #{total_time} second(s)"
end

main() if __FILE__ == $0
