$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), 'support'))

# needs to be at the top
require 'simplecov'
SimpleCov.start do
  add_filter "/thrift/"
end

Bundler.require :default, :test


require 'rubygems'
require 'bundler'
require 'awesome_print'

require 'ok-hbase'


