module OkHbase
  class Row
    attr_accessor :table, :row_key, :timestamp, :default_column_family

    def initialize(opts={})

      opts = opts.with_indifferent_access

      raise ArgumentError.new "'table' must be a  OkHBase::Table" unless opts[:table] &&  opts[:table].is_a?(OkHbase::Table)

      @table = opts[:table]

      @row_key = opts[:row_key]
      @data = opts[:data]
      @default_column_family = opts[:default_column_family]

    end

    def save!
      @table.put(@row_key, @data, @timestamp)
    end

    def method_missing(method, *arguments, &block)


      if method.to_s[-1, 1] == '='
        method = :[]=
        key = arguments.first
        val = arguments.last
        key = "#{@default_column_family}:#{key}" unless key.to_s.include? ':'
        return @data.send(method, key, val)
      else

        key = "#{@default_column_family}:#{method}" unless method.to_s.include? ':'
        method = :[]
        return @data.send(method, key)
      end

    end
  end
end
