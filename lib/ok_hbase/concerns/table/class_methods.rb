module OkHbase
  module Concerns
    module Table
      module ClassMethods
        extend ActiveSupport::Concern

        module ClassMethods
          include OkHbase::Concerns::Table
        end
      end
    end
  end
end
