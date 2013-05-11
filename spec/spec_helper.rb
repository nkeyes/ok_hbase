$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), 'support'))

Bundler.require :default, :test

require 'simplecov'
require 'rubygems'
require 'bundler'
require 'awesome_print'

require 'ok-hbase'

SimpleCov.start
