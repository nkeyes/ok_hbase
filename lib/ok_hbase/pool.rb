require 'thread'
require 'timeout'

require 'ok_hbase/connection'
require 'ok_hbase/no_connections_available'


module OkHbase
  class Pool
    @_lock
    @_connection_queue
    @_connection_ids

    def initialize(size, opts={})
      raise TypeError.new("'size' must be an integer") unless size.is_a? Integer
      raise ArgumentError.new("'size' must be > 0") unless size > 0

      OkHbase.logger.debug("Initializing connection pool with #{size} connections.")

      @_lock = Mutex.new
      @_connection_queue = Queue.new
      @_connection_ids = []

      connection_opts = opts.dup

      connection_opts[:auto_connect] = false

      size.times do
        connection = OkHbase::Connection.new(connection_opts)
        @_connection_queue << connection
        @_connection_ids << connection.object_id
      end

      # The first connection is made immediately so that trivial
      # mistakes like unresolvable host names are raised immediately.
      # Subsequent connections are connected lazily.
      self.with_connection {} if opts[:auto_connect]
    end

    def synchronize(&block)
      @_lock.synchronize(&block)
    end

    def with_connection(timeout = nil)
      connection = Thread.current[:ok_hbase_current_connection]

      return_after_use = false

      begin
        unless connection
          return_after_use = true
          connection = get_connection(timeout)
          Thread.current[:ok_hbase_current_connection] = connection
        end
        yield connection
      rescue Apache::Hadoop::Hbase::Thrift::IOError, Thrift::TransportException, SocketError => e
        raise e
      ensure
        if return_after_use
          Thread.current[:ok_hbase_current_connection] = nil
          return_connection(connection)
        end
      end
    end

    def get_connection(timeout = nil)
      begin
        connection = Timeout.timeout(timeout) do
          @_connection_queue.deq
        end
      rescue TimeoutError
        raise OkHbase::NoConnectionsAvailable.new("No connection available from pool within specified timeout: #{timeout}")
      end
      begin
        connection.open()
        connection.reset unless connection.ping?
      rescue Apache::Hadoop::Hbase::Thrift::IOError, Thrift::TransportException, SocketError => e
        connection.reset
        raise e
      end
      connection
    end

    def return_connection(connection)
      synchronize do
        return unless @_connection_ids.include? connection.object_id
      end

      @_connection_queue << connection
    end
  end
end
