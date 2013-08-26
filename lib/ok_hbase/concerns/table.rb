require 'ok_hbase/concerns/table/batch'

module OkHbase
  module Concerns
    module Table
      extend ActiveSupport::Concern

      SCANNER_DEFAULTS = {
          start_row: nil,
          stop_row: nil,
          row_prefix: nil,
          columns: nil,
          filter_string: nil,
          timestamp: nil,
          include_timestamp: false,
          caching: 1000,
          limit: nil,
      }.freeze

      attr_accessor :table_name, :connection

      def table_name
        @table_name
      end

      def table_name=(val)
        @table_name = val
      end

      def self.connection
        @self.connection
      end

      def self.connection=(val)
        @self.connection = val
      end


      def families()
        descriptors = self.connection.client.getColumnDescriptors(self.connection.table_name(table_name))

        families = {}

        descriptors.each_pair do |name, descriptor|
          name = name[0...-1] # remove trailing ':'
          families[name] = OkHbase.thrift_type_to_dict(descriptor)
        end
        families
      end

      def regions
        regions = self.connection.client.getTableRegions(self.connection.table_name(table_name))
        regions.map { |r| OkHbase.thrift_type_to_dict(r) }
      end

      def row(row_key, columns = nil, timestamp = nil, include_timestamp = false)
        raise TypeError.new "'columns' must be a tuple or list" if columns && !columns.is_a?(Array)

        row_key.force_encoding(Encoding::UTF_8)

        rows = if timestamp
          raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

          self.connection.client.getRowWithColumnsTs(self.connection.table_name(table_name), row_key, columns, timestamp, {})
        else
          self.connection.client.getRowWithColumns(self.connection.table_name(table_name), row_key, columns, {})
        end

        rows.empty? ? {} : _make_row(rows[0].columns, include_timestamp)
      end

      def rows(row_keys, columns = nil, timestamp = nil, include_timestamp = false)
        raise TypeError.new "'columns' must be a tuple or list" if columns && !columns.is_a?(Array)

        row_keys.map! { |r| r.force_encoding(Encoding::UTF_8) }

        return {} if row_keys.blank?

        rows = if timestamp
          raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

          columns = _column_family_names() unless columns

          self.connection.client.getRowsWithColumnsTs(self.connection.table_name(table_name), row_keys, columns, timestamp, {})
        else
          self.connection.client.getRowsWithColumns(self.connection.table_name(table_name), row_keys, columns, {})
        end

        rows.map { |row| [row.row, _make_row(row.columns, include_timestamp)] }
      end

      def cells(row_key, column, versions = nil, timestamp = nil, include_timestamp = nil)

        row_key.force_encoding(Encoding::UTF_8)

        versions ||= (2 ** 31) -1

        raise TypeError.new "'versions' parameter must be a number or nil" unless versions.is_a? Integer
        raise ArgumentError.new "'versions' parameter must be >= 1" unless versions >= 1

        cells = if timestamp
          raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

          self.connection.client.getVerTs(self.connection.table_name(table_name), row_key, column, timestamp, versions, {})
        else
          self.connection.client.getVer(self.connection.table_name(table_name), row_key, column, versions, {})
        end

        cells.map { |cell| include_timestamp ? [cell.value, cell.timestamp] : cell.value }
      end

      def scan(opts={})

        rows = [] unless block_given?
        opts = SCANNER_DEFAULTS.merge opts.select { |k| SCANNER_DEFAULTS.keys.include? k }


        raise ArgumentError.new "'caching' must be >= 1" unless opts[:caching] && opts[:caching] >= 1
        raise ArgumentError.new "'limit' must be >= 1" if opts[:limit] && opts[:limit] < 1

        if opts[:row_prefix]
          raise ArgumentError.new "'row_prefix' cannot be combined with 'start_row' or 'stop_row'" if opts[:start_row] || opts[:stop_row]

          opts[:start_row] = opts[:row_prefix]
          opts[:stop_row] = OkHbase::increment_string opts[:start_row]

        end
        opts[:start_row] ||= ''

        scanner = _scanner(opts)

        scanner_id = self.connection.client.scannerOpenWithScan(self.connection.table_name(table_name), scanner, {})

        fetched_count = returned_count = 0

        begin
          while true
            how_many = opts[:limit] ? [opts[:caching], opts[:limit] - returned_count].min : opts[:caching]

            items = if how_many == 1
              self.connection.client.scannerGet(scanner_id)
            else
              self.connection.client.scannerGetList(scanner_id, how_many)
            end

            fetched_count += items.length

            items.map.with_index do |item, index|
              if block_given?
                yield item.row, _make_row(item.columns, opts[:include_timestamp])
              else
                rows << [item.row, _make_row(item.columns, opts[:include_timestamp])]
              end
              return rows if opts[:limit] && index + 1 + returned_count == opts[:limit]
            end

            break if items.length < how_many
          end
        ensure
          self.connection.client.scannerClose(scanner_id)
        end
        rows
      end

      def put(row_key, data, timestamp = nil)
        batch = self.batch(timestamp)

        batch.transaction do |batch|
          batch.put(row_key, data)
        end
      end

      def delete(row_key, columns = nil, timestamp = nil)
        if columns
          batch = self.batch(timestamp)
          batch.transaction do |batch|
            batch.delete(row_key, columns)
          end

        else
          timestamp ? self.connection.client.deleteAllRowTs(self.connection.table_name(table_name), row_key, timestamp, {}) : self.connection.client.deleteAllRow(self.connection.table_name(table_name), row_key, {})
        end
      end

      def batch(timestamp = nil, batch_size = nil, transaction = false)
        Batch.new(self, timestamp, batch_size, transaction)
      end

      def counter_get(row_key, column)
        counter_inc(row_key, column, 0)
      end

      def counter_set(row_key, column, value = 0)
        self.batch.transaction do |batch|
          batch.put(row_key, { column => [value].pack('Q>') })
        end
      end

      def counter_inc(row_key, column, value = 1)
        self.connection.client.atomicIncrement(self.connection.table_name(table_name), row_key, column, value)
      end

      def counter_dec(row_key, column, value = 1)
        counter_inc(row_key, column, -value)
      end

      def increment(increment)
        self.connection.increment(_new_increment(increment))
      end

      def increment_rows(increments)
        increments.map! { |i| _new_increment(i) }
        self.connection.increment(_new_increment(increments))
      end

      alias_method :find, :scan

      def _column_family_names()
        self.connection.client.getColumnDescriptors(self.connection.table_name(table_name)).keys()
      end

      def _scanner(opts)
        scanner = Apache::Hadoop::Hbase::Thrift::TScan.new()
        scanner_fields = Apache::Hadoop::Hbase::Thrift::TScan::FIELDS

        opts.each_pair do |k, v|
          const = k.to_s.upcase.gsub('_', '')
          const_value = Apache::Hadoop::Hbase::Thrift::TScan.const_get(const) rescue nil

          if const_value
            v.force_encoding(Encoding::UTF_8) if v.is_a?(String)
            OkHbase.logger.info "setting scanner.#{scanner_fields[const_value][:name]}: #{v}"
            scanner.send("#{scanner_fields[const_value][:name]}=", v)
          else
          end
        end
        scanner

      end

      def _make_row(cell_map, include_timestamp)
        row = {}
        cell_map.each_pair do |cell_name, cell|
          row[cell_name] = include_timestamp ? [cell.value, cell.timestamp] : cell.value
        end
        row
      end

      def _new_increment(args)
        args[:table] = self.table_name
        args
      end
    end
  end
end
