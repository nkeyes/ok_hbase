require 'thrift'
require 'thrift/transport/socket'
require 'thrift/protocol/binary_protocol'

require 'thrift/hbase'

module OkHbase
  class Client < Apache::Hadoop::Hbase::Thrift::Hbase::Client

    attr_accessor :max_tries

    def initialize(iprot, oprot=nil, max_tries=nil)
      @max_tries = max_tries || 0
      super(iprot, oprot)
    end

    signatures = ['send_message(name, args_class, args = {})', 'receive_message(result_klass)']

    signatures.each do |signature|
      module_eval <<-RUBY, __FILE__, __LINE__
        def #{signature}
          tries = 0
          begin
            @iprot.trans.open unless @iprot.trans.open?
            super
          rescue => e
            tries += 1
            raise e unless tries < max_tries && recoverable?(e)
            retry
          end
        end
      RUBY
    end

    def recoverable?(e)
      e.is_a?(Apache::Hadoop::Hbase::Thrift::IOError) ||
          e.is_a?(Thrift::TransportException)
    end
  end
end
