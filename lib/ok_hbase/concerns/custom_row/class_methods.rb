module OkHbase
  module Concerns
    module CustomRow
      module ClassMethods
        extend ActiveSupport::Concern

        module ClassMethods
          include OkHbase::Concerns::CustomRow
        end
      end
    end
  end
end
