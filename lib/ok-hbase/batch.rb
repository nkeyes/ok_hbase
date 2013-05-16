module OkHbase
  class Batch

    attr_reader :table, :batch_size, :timestamp

    def initialize(table, timestamp = nil, batch_size = nil, transaction = false)
      raise TypeError.new "'timestamp' must be an integer or nil" if timestamp && !timestamp.is_a?(Integer)

      if batch_size
        raise ArgumentError.new "'transaction' cannot be used when 'batch_size' is specified" if transaction
        raise ValueError.new "'batch_size' must be > 0" unless batch_size > 0
      end

      @table = table
      @batch_size = batch_size
      @timestamp = timestamp
      @transaction = transaction
      @families = nil

      _reset_mutations()

    end


    def send_batch()
      batch_mutations = @mutations.map do |row_key, mutations|
        Apache::Hadoop::Hbase::Thrift::BatchMutation.new(row: row_key, mutations: mutations)
      end

      return if batch_mutations.blank?

      @timestamp ? @table.connection.client.mutateRowsTs(table.name, batch_mutations, @timestamp) : @table.connection.client.mutateRows(@table.name, batch_mutations)

      _reset_mutations()
    end

    def put(row_key, data)
      @mutations[row_key] ||= []

      data.each_pair do |column, value|
        @mutations[row_key] << Apache::Hadoop::Hbase::Thrift::Mutation.new(isDelete: false, column: column, value: value)
      end

      @mutation_count += data.size

      send_batch if @batch_size && @mutation_count > @batch_size
    end

    def delete(row_key, columns = nil)
      columns ||= @families ||= @table.send(:_column_family_names)

      @mutations[row_key] ||= []

      columns.each do |column|
        @mutations[row_key] << Apache::Hadoop::Hbase::Thrift::Mutation.new(isDelete: true, column: column)
      end

      @mutation_count += columns.size
      send_batch if @batch_size && @mutation_count > @batch_size
    end

    def transaction
      yield self
      send_batch
    end

    private

    def _reset_mutations
      @mutations = {}
      @mutation_count = 0
    end
  end
end
