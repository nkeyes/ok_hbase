module OkHbase
  module Concerns
    module Table
      module ClassMethods
        extend ActiveSupport::Concern



        module ClassMethods
          include OkHbase::Concerns::Table

          @@table_name ||= nil
          @@connection ||= nil

          def table_name
            @@table_name
          end

          def table_name=(val)
            @@table_name = val
          end

          def connection
            @@connection
          end

          def connection=(val)
            @@connection = val
          end


        end
      end
    end
  end
end
