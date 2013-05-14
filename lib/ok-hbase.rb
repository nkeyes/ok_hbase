require 'active_support/all'
require 'ok-hbase/version'
require 'ok-hbase/connection'
require 'ok-hbase/batch'
require 'ok-hbase/table'

module OkHbase

  mattr_accessor :logger

  def self.root
    ::Pathname.new File.expand_path('../../', __FILE__)
  end

  def self.logger
    @@logger ||= init_logger
  end

  def self.init_logger
    Logger.new("/dev/null")
  end

  def self.increment_string(string)
    bytes = string.bytes.to_a
    (0...bytes.length).to_a.reverse.each do |i|
      return (bytes[0...i] << bytes[i]+1).pack('C*').force_encoding(Encoding::UTF_8) unless bytes[i] == 255
    end
    nil
  end

  def self.thrift_type_to_dict(obj)
    Hash[obj.class::FIELDS.map{ |k, v| [v[:name].underscore, obj.send(v[:name])]}]
  end
end
