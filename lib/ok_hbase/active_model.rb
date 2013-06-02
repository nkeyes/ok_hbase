require 'ok_hbase/concerns'
require 'ok_hbase/row'

module OkHbase
  class ActiveModel < OkHbase::Row
    include OkHbase::Concerns::Table::ClassMethods
    include OkHbase::Concerns::CustomRow::ClassMethods
    include OkHbase::Concerns::Indexable::ClassMethods
    include OkHbase::Concerns::Table::Instrumentation

    def initialize(raw_data={})

      raw_data = raw_data.with_indifferent_access
      raw_data = raw_data[:raw_data] if raw_data[:raw_data]


      options = {
          table: self.class,
          default_column_family: self.class.default_column_family,
          raw_data: raw_data,
      }
      super(options)
    end

    def self.create(raw_data={})
      instance = new(raw_data)
      instance.save!
      instance
    end

    def delete(indexes=[])
      self.class.delete(row_key, nil, nil, indexes)
    end
  end
end
