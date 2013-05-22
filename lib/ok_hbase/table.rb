module OkHbase
  class Table
    include OkHbase::Concerns::Table

    def initialize(name, connection)
      @connection = connection
      @table_name = name
    end
  end
end
