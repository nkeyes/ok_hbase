require 'thrift'
require 'thrift/transport/socket'
require 'thrift/protocol/binary_protocol'

require 'thrift/hbase'

require 'ok_hbase/client'

module OkHbase
  class Connection


    DEFAULT_OPTS = {
        host: 'localhost',
        port: 9090,
        timeout: 5,
        auto_connect: false,
        table_prefix: nil,
        table_prefix_separator: '_',
        transport: :buffered,
        max_tries: 3
    }.freeze

    THRIFT_TRANSPORTS = {
        buffered: Thrift::BufferedTransport,
        framed: Thrift::FramedTransport,
    }

    attr_accessor :host, :port, :timeout, :auto_connect, :table_prefix, :table_prefix_separator, :max_tries
    attr_reader :client

    def initialize(opts={})
      opts = DEFAULT_OPTS.merge opts

      raise ArgumentError.new ":transport must be one of: #{THRIFT_TRANSPORTS.keys}" unless THRIFT_TRANSPORTS.keys.include?(opts[:transport])
      raise TypeError.new ":table_prefix must be a string" if opts[:table_prefix] && !opts[:table_prefix].is_a?(String)
      raise TypeError.new ":table_prefix_separator must be a string" unless opts[:table_prefix_separator].is_a?(String)


      @host = opts[:host]
      @port = opts[:port]
      @timeout = opts[:timeout]
      @max_tries = opts[:max_tries]
      @auto_connect = opts[:auto_connect]
      @table_prefix = opts[:table_prefix]
      @table_prefix_separator = opts[:table_prefix_separator]
      @transport_class = THRIFT_TRANSPORTS[opts[:transport]]

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

    def table(name, use_prefix=true)
      name = table_name(name) if use_prefix
      OkHbase::Table.new(name, self)
    end

    def tables
      names = client.getTableNames
      if table_prefix
        names = names.map do |n|
          n["#{table_prefix}#{table_prefix_separator}".size..-1] if n.start_with?(table_prefix)
        end
      end
      names
    end

    def create_table(name, families)
      name = table_name(name)

      raise ArgumentError.new "Can't create table #{name}. (no column families specified)" unless families
      raise TypeError.new "families' arg must be a hash" unless families.respond_to?(:[])

      column_descriptors = []

      families.each_pair do |family_name, options|
        options ||= {}

        args = {}
        options.each_pair do |option_name, value|
          args[option_name.to_s.camelcase(:lower)] = value
        end

        family_name = "#{family_name}:" unless family_name.to_s.end_with? ':'
        args[:name] = family_name

        column_descriptors << Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new(args)
      end

      client.createTable(name, column_descriptors)
      table(name)
    end

    def delete_table(name, disable=false)
      name = table_name(name)

      disable_table(name) if disable && table_enabled?(name)
      client.deleteTable(name)
    end

    def enable_table(name)
      name = table_name(name)

      client.enableTable(name)
    end

    def disable_table(name)
      name = table_name(name)

      client.disableTable(name)
    end

    def table_enabled?(name)
      name = table_name(name)

      client.isTableEnabled(name)
    end

    def compact_table(name, major=false)
      name = table_name(name)

      major ? client.majorCompact(name) : client.compact(name)
    end

    def table_name(name)
      table_prefix && !name.start_with?(table_prefix) ? [table_prefix, name].join(table_prefix_separator) : name
    end

    def increment(increment)
      client.increment(_new_increment(increment))
    end

    def increment_rows(increments)
      increments.map! { |i| _new_increment(i) }
      client.incrementRows(increments)
    end

    private

    def _refresh_thrift_client
      socket = Thrift::Socket.new(host, port, timeout)
      @transport = @transport_class.new(socket)
      protocol = Thrift::BinaryProtocolAccelerated.new(@transport)
      @client = OkHbase::Client.new(protocol, nil, max_tries)
    end

    def _new_increment(args)
      if args[:amount]
        args[:ammount] ||= args[:amount]
        args.delete(:amount)
      end
      Apache::Hadoop::Hbase::Thrift::TIncrement.new(args)
    end
  end
end
