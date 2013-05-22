module OkHbase
  module Concerns
    module Indexable
      module ClassMethods
        extend ActiveSupport::Concern

        module ClassMethods
          include OkHbase::Concerns::Indexable
        end
      end
    end
  end
end
