module OkHbase
  module Concerns
    module CustomRow
      extend ActiveSupport::Concern

      def row(row_key, columns = nil, timestamp = nil, include_timestamp = false)
        self.row_class.new table: self, default_column_family: self.default_column_family, raw_data: super
      end

      def rows(row_keys, columns = nil, timestamp = nil, include_timestamp = false)
        super.map { |row_key, data| self.row_class.new table: self, row_key: row_key, default_column_family: self.default_column_family, raw_data: data }
      end

      def scan(opts={})
        if block_given?
          super { |row_key, data| yield self.row_class.new(table: self, row_key: row_key, default_column_family: self.default_column_family, raw_data: data) }
        else
          super.map { |row_key, data| self.row_class.new(table: self, row_key: row_key, default_column_family: self.default_column_family, raw_data: data) }
        end

      end

      def row_class
        @@_row_class
      end

      def default_column_family
        @@default_column_family
      end

      def use_row_class(klass)
        @@_row_class = klass
      end

      def use_default_column_family(column_family)
        @@default_column_family = column_family
      end
    end
  end
end
