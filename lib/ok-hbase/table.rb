module OkHbase
  class Table

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

    attr_accessor :name, :connection

    def initialize(name, connection)
      @name = name
      @connection = connection
    end

    def families()
      descriptors = connection.client.getColumnDescriptors(name)

      families = {}

      descriptors.each_pair do |name, descriptor|
        name = name[0...-1] # remove trailing ':'
        families[name] = OkHbase.thrift_type_to_dict(descriptor)
      end
      families
    end

    def regions
      regions = connection.client.getTableRegions(name)
      regions.map { |r| OkHbase.thrift_type_to_dict(r) }
    end

    def row(row_key, columns = nil, timestamp = nil, include_timestamp = false)
      raise TypeError.new "'columns' must be a tuple or list" if columns && !columns.is_a?(Array)

      row_key.force_encoding(Encoding::UTF_8)

      rows = if timestamp
        raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

        connection.client.getRowWithColumnsTs(name, row_key, columns, timestamp)
      else
        connection.client.getRowWithColumns(name, row_key, columns)
      end

      rows ? self.class._make_row(rows[0].columns, include_timestamp) : {}
    end

    def rows(row_keys, columns = nil, timestamp = nil, include_timestamp = false)
      raise TypeError.new "'columns' must be a tuple or list" if columns && !columns.is_a?(Array)

      row_keys.map! { |r| r.force_encoding(Encoding::UTF_8) }

      return {} if row_keys.empty?

      rows = if timestamp
        raise TypeError.new "'timestamp' must be an integer" unless timestamp.is_a? Integer

        columns = _column_family_names() unless columns

        connection.client.getRowsWithColumnsTs(name, row_keys, columns, timestamp)
      else
        connection.client.getRowsWithColumns(name, row_keys, columns)
      end

      rows.map { |row| self.class._make_row(row.columns, include_timestamp) }
    end

    def scan(opts={})
      opts = SCANNER_DEFAULTS.merge opts.select { |k| SCANNER_DEFAULTS.keys.include? k }


      raise ArgumentError.new "'caching' must be >= 1" unless opts[:caching] && opts[:caching] >= 1
      raise ArgumentError.new "'limit' must be >= 1" if opts[:limit] && opts[:limit] < 1

      if opts[:row_prefix]
        raise ArgumentError.new "'row_prefix' cannot be combined with 'start_row' or 'stop_row'" if opts[:start_row] || opts[:stop_row]

        opts[:start_row] = opts[:row_prefix]
        opts[:stop_row] = OkHbase::increment_string opts[:start_row]

      end
      opts[:start_row] ||= ''

      scanner = self.class._scanner(opts)


      scanner_id = connection.client.scannerOpenWithScan(name, scanner)

      fetched_count = returned_count = 0

      begin
        while true
          how_many = opts[:limit] ? [opts[:caching], opts[:limit] - returned_count].min : opts[:caching]

          items = if how_many == 1
            connection.client.scannerGet(scanner_id)
          else
            connection.client.scannerGetList(scanner_id, how_many)
          end

          fetched_count += items.length

          items.map.with_index do |item, index|
            yield item.row, self.class._make_row(item.columns, opts[:include_timestamp])
            return if opts[:limit] && index + 1 + returned_count == opts[:limit]
          end

          break if items.length < how_many
        end
      ensure
        connection.client.scannerClose(scanner_id)
      end
    end

    alias_method :find, :scan

    private
    def _column_family_names()
      connection.client.getColumnDescriptors(name).keys()
    end

    def self._scanner(opts)
      scanner = Apache::Hadoop::Hbase::Thrift::TScan.new()
      scanner_fields = Apache::Hadoop::Hbase::Thrift::TScan::FIELDS

      opts.each_pair do |k, v|
        const = k.to_s.upcase.gsub('_', '')
        const_value = Apache::Hadoop::Hbase::Thrift::TScan.const_get(const) rescue nil

        if const_value
          OkHbase.logger.info "setting scanner.#{scanner_fields[const_value][:name]}: #{v}"
          scanner.send("#{scanner_fields[const_value][:name]}=", v)
        else
        end
      end
      scanner

    end

    def self._make_row(cell_map, include_timestamp)
      row = {}
      cell_map.each_pair do |cell_name, cell|
        row[cell_name] = cell.value
      end
      row
    end
  end
end
