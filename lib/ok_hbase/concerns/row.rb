module OkHbase
  module Concerns
    module Row
      attr_accessor :table, :row_key, :timestamp, :default_column_family
      attr_reader :raw_data

      def id
        self.row_key
      end

      def id=(val)
        self.row_key = val
      end

      def encoded_data
        Hash[@raw_data.map { |k, v| [k, _encode(v)] }].with_indifferent_access
      end

      def attributes
        hash = Hash[@raw_data.keys.map do |k|
          k = k.split(':', 2).last
          key_value = [k, send(k)]
          key_value
        end
        ].with_indifferent_access

        hash[:row_key] = @row_key
        hash

      end

      def save!()
        #raise ArgumentError.new "row_key must be a non-empty string" unless !@row_key.blank? && @row_key.is_a?(String)

        table.put(row_key, encoded_data, timestamp)
      end

      def delete
        table.delete(row_key)
      end

      def increment(increment)
        self.table.increment(_new_increment(increment))
      end

      def method_missing(method, *arguments, &block)
        if method.to_s[-1, 1] == '='

          key = method[0...-1]
          val = arguments.last
          unless key.to_s.include? ':'
            key = "#{default_column_family}:#{key}"
          else
          end

          ret_val = raw_data[key] = val

          ret_val
        else

          unless method.to_s.include? ':'
            key = "#{default_column_family}:#{method}"
          else
          end
          return raw_data[key]
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
          when NilClass
            value
        end

        encoded
      end

      def _new_increment(args)
        args[:row] = self.row_key
        args
      end

    end
  end
end
