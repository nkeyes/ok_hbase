$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), 'support'))

require 'simplecov'

SimpleCov.start

require 'rubygems'
require 'bundler'
require 'json'
require 'yaml'

Bundler.require :default, :test

require 'ok-hbase'
