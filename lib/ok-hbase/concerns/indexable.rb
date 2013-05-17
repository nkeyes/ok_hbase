require 'ok-hbase/concerns/custom_row'

module OkHbase
  module Concerns
    module Indexable
      extend ActiveSupport::Concern
      include OkHbase::Concerns::CustomRow

      module ClassMethods
        def use_index(index_name, opts={})
          options = opts.with_indifferent_access
          attributes = options[:attributes]
          prefix_length = options[:prefix_length]
          index_id = options[:index_id]
          pack_pattern = options[:pack_pattern]
          auto_create = options[:auto_create]

          define_method index_name do |idx_options, &block|
            expected_option_keys = attributes[0...prefix_length]
            prefix_pack_pattern = pack_pattern[0...prefix_length].join('')

            prefix_components = expected_option_keys.map do |key|
              key == :index_id ? index_id : idx_options[key]
            end

            row_prefix = prefix_components.pack(prefix_pack_pattern)

            scan(row_prefix: row_prefix, &block)
          end

        end
      end


    end
  end
end
