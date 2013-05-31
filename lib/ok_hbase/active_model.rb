require 'ok_hbase/concerns'
require 'ok_hbase/row'

module OkHbase
  class ActiveModel < OkHbase::Row
    include OkHbase::Concerns::Table::ClassMethods
    include OkHbase::Concerns::CustomRow::ClassMethods
    include OkHbase::Concerns::Indexable::ClassMethods
    include OkHbase::Concerns::Table::Instrumentation
  end
end
