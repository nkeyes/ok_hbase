module OkHbase
  class Table

    attr_accessor :name, :connection

    def initialize(name, connection)
      @name = name
      @connection = connection
    end

    def scan(opts={})
      opts_defaults = {
          start_row: nil,
          stop_row: nil,
          row_prefix: nil,
          columns: nil,
          filter_string: nil,
          timestamp: nil,
          include_timestamp: false,
          caching: 1000,
          limit: nil,
      }
      opts = opts_defaults.merge opts.select { |k| opts_defaults.keys.include? k }


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
