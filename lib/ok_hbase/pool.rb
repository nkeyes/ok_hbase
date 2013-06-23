require 'thread'
require 'timeout'

require 'ok_hbase/connection'
require 'ok_hbase/no_connections_available'


module OkHbase
  class Pool
    @_lock
    @_connection_queue

    def initialize(size, opts={})
      raise TypeError.new("'size' must be an integer") unless size.is_a? Integer
      raise ArgumentError.new("'size' must be > 0") unless size > 0

      OkHbase.logger.debug("Initializing connection pool with #{size} connections.")

      @_lock = Mutex.new
      @_connection_queue = Queue.new

      connection_opts = opts.dup

      connection_opts[:auto_connect] = false

      size.times do
        connection = OkHbase::Connection.new(connection_opts)
        @_connection_queue << connection
      end

      # The first connection is made immediately so that trivial
      # mistakes like unresolvable host names are raised immediately.
      # Subsequent connections are connected lazily.
      self.connection {} if opts[:auto_connect]
    end

    def connection(timeout = nil)
      connection = Thread.current[:ok_hbase_current_connection]

      return_after_use = false

      unless connection
        return_after_use = true
        connection = _acquire_connection(timeout)
        @_lock.synchronize do
          Thread.current[:ok_hbase_current_connection] = connection
        end
      end

      begin
        connection.open()
        yield connection
      rescue Apache::Hadoop::Hbase::Thrift::IOError, Thrift::TransportException, SocketError => e
        OkHbase.logger.info("Replacing tainted pool connection")

        connection.send(:_refresh_thrift_client)
        connection.open
        raise e
      ensure
        if return_after_use
          Thread.current.delete[:ok_hbase_current_connection]
          _return_connection(connection)
        end
      end
    end

    private

    def _acquire_connection(timeout = nil)
      begin
        Timeout.timeout(timeout) do
          return @_connection_queue.deq
        end
      rescue TimeoutError
        raise OkHbase::NoConnectionsAvailable.new("No connection available from pool within specified timeout: #{timeout}")
      end
    end

    def _return_connection(connection)
      @_connection_queue << connection
    end
  end
end
