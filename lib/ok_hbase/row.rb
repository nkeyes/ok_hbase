require 'ok_hbase/concerns/row'
module OkHbase
  class Row
    include OkHbase::Concerns::Row

    def initialize(opts={})

      opts = opts.with_indifferent_access

      raise ArgumentError.new "'table' must be an OkHBase::Table" unless opts[:table] && opts[:table].is_a?(OkHbase::Table)
      @default_column_family = opts[:default_column_family]

      @table = opts[:table]

      @row_key = opts[:row_key]
      @raw_data = {}.with_indifferent_access
      opts[:raw_data].each_pair do |k, v|

        send(:"#{k}=", v)
      end
    end

  end
end
