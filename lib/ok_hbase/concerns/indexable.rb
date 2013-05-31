module OkHbase
  module Concerns
    module Indexable
      extend ActiveSupport::Concern

      def use_index(index_name, opts={})
        options = opts.with_indifferent_access
        attributes = options[:attributes]
        prefix_length = options[:prefix_length]
        index_id = options[:index_id]
        pack_pattern = options[:pack_pattern]
        auto_create = options[:auto_create]

        @@_indexes ||= {}
        @@_indexes = @@_indexes.with_indifferent_access
        @@_indexes[index_name] = options

        define_singleton_method :indexes do
          @@_indexes
        end


        define_singleton_method :encode_for_row_key do |value|
          # coerce booleans to ints for packing
          value = 1 if value.to_s.downcase == "true"
          value = 0 if value.to_s.downcase == "false"

          # coerce hbase i64s to Fixnum, Bignum
          value = value.unpack('Q>').first if value.is_a?(String)

          value
        end

        define_singleton_method :key_for_index do |index_name, data|

          options = @@_indexes[index_name]

          row = self.row_class.new(table: self, default_column_family: self.default_column_family, raw_data: data)
          row_key_components = options[:attributes].map do |attribute|

            value = if attribute == :index_id
              options[:index_id]
            else
              row.attributes[attribute] || row.send(attribute)
            end
            encode_for_row_key(value)
          end

          row_key_components.pack(options[:pack_pattern].join(''))

        end

        define_singleton_method index_name do |idx_options, &block|
          expected_option_keys = attributes[0...prefix_length]
          prefix_pack_pattern = pack_pattern[0...prefix_length].join('')

          prefix_components = expected_option_keys.map do |key|
            value = key == :index_id ? index_id : idx_options[key]
            encode_for_row_key(value)
          end

          row_prefix = prefix_components.pack(prefix_pack_pattern)

          scan(row_prefix: row_prefix, &block)
        end
      end

      def put(row_key, data, timestamp = nil, extra_indexes=[])
        batch(timestamp).transaction do |batch|
          @@_indexes.each_pair do |index_name, options|
            next unless options[:auto_create] || extra_indexes.include?(index_name)

            index_row_key = key_for_index(index_name, data)

            batch.put(index_row_key, data)
          end
        end
      end

      def delete(row_key, columns=nil, timestamp=nil, indexes=[])
        row = self.row(row_key)
        attributes = row.attributes
        if attributes[:row_key].blank? && attributes.except(:row_key).blank?
          return
        end

        indexes = Array(indexes)

        if indexes.empty?
          indexes = @@_indexes.keys
        end
        self.batch(timestamp).transaction do |batch|
          indexes.each do |index_name|
            index_row_key = key_for_index(index_name, row.attributes)
            batch.delete(index_row_key, columns)
          end
        end
      end
    end
  end
end
