# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ok_hbase/version'

Gem::Specification.new do |gem|
  gem.name          = "ok_hbase"
  gem.version       = OkHbase::VERSION
  gem.authors       = ["Nathan Keyes"]
  gem.email         = ["keyes@okcupidlabs.com"]
  gem.description   = %q{Lightweight Ruby Hbase Client}
  gem.summary       = %q{Lightweight Ruby Hbase Client}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]


  gem.add_dependency 'thrift', '0.9.0'
  gem.add_dependency 'activesupport'
end
