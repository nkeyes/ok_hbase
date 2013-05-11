require "ok-hbase/version"
require "ok-hbase/connection"

module OkHbase
  def self.increment_string(string)
    bytes = string.bytes.to_a
    (0...bytes.length).to_a.reverse.each do |i|
      return (bytes[0...i] << bytes[i]+1).pack('U*') unless bytes[i] == 255
    end
    nil
  end
end
