module OkHbase
  class Row
    attr_accessor :table, :row_key, :raw_data, :timestamp, :default_column_family

    def initialize(opts={})

      opts = opts.with_indifferent_access

      raise ArgumentError.new "'table' must be an OkHBase::Table" unless opts[:table] && opts[:table].is_a?(OkHbase::Table)
      @default_column_family = opts[:default_column_family]

      @table = opts[:table]

      @row_key = opts[:row_key]
      @raw_data = {}
      opts[:raw_data].each_pair do |k, v|

        send(:"#{k}=", v)
      end
    end

    def save!
      #raise ArgumentError.new "row_key must be a non-empty string" unless !@row_key.blank? && @row_key.is_a?(String)

      encoded_data = Hash[@raw_data.map { |k, v| [k, _encode(v)] }]
      @table.put(@row_key, encoded_data, @timestamp)
    end

    def method_missing(method, *arguments, &block)

      if method.to_s[-1, 1] == '='

        key = method[0...-1]
        val = arguments.last
        unless key.to_s.include? ':'
          key = "#{@default_column_family}:#{key}"
        else
        end

        ret_val = @raw_data[key] = val

        ret_val
      else

        unless method.to_s.include? ':'
          key = "#{@default_column_family}:#{method}"
        else
        end
        return @raw_data[key]
      end

    end

    private
    def _encode(value)
      encoded = case value
        when String
          value.dup.force_encoding(Encoding::UTF_8)
        when Bignum, Fixnum
          [value].pack('Q>').force_encoding(Encoding::UTF_8)
        when TrueClass, FalseClass
          value.to_s.force_encoding(Encoding::UTF_8)
        when NilClass, TrueClass, FalseClass
          value
      end

      encoded
    end

  end
end
