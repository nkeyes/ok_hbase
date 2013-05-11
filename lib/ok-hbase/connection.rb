require 'thrift'
require 'thrift/transport/socket'
require 'thrift/protocol/binary_protocol'

require 'ok-hbase/thrift/hbase/hbase_constants'
require 'ok-hbase/thrift/hbase/hbase_types'
require 'ok-hbase/thrift/hbase/hbase'

module OkHbase
  class Connection

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 9090
    DEFAULT_TIMEOUT = 5

    attr_accessor :host, :port, :timeout, :auto_connect
    attr_reader :client

    def initialize(opts={})
      opts = {
          host: DEFAULT_HOST,
          port: DEFAULT_PORT,
          timeout: DEFAULT_TIMEOUT,
          auto_connect: false
      }.merge opts

      @host = opts[:host]
      @port = opts[:port]
      @timeout = opts[:timeout]
      @auto_connect = opts[:auto_connect]

      _refresh_thrift_client
      open if @auto_connect

    end

    def open
      return if open?
      @transport.open

      OkHbase.logger.info "OkHbase connected"
    end

    def open?
      @transport && @transport.open?
    end

    def close
      return unless open?
      @transport.close
    end

    def tables
      @client.getTableNames
    end

    private

    def _refresh_thrift_client
      socket = Thrift::Socket.new(host, port, timeout)
      @transport = Thrift::BufferedTransport.new(socket)
      protocol = Thrift::BinaryProtocolAccelerated.new(@transport)
      @client = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(protocol)

    end
  end
end
