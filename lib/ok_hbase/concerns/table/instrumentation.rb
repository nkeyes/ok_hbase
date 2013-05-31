module OkHbase
  module Concerns
    module Table
      module Instrumentation
        extend ActiveSupport::Concern

        module ClassMethods
          instrumented_methods = {
              load: %w[row rows cells scan],
          }

          instrumented_methods.each_pair do |method_type, methods|
            methods.each do |method_to_instrument|
              module_eval <<-RUBY, __FILE__, __LINE__
              def #{method_to_instrument}(*args)
                opts = {
                  name: [table_name, '#{method_type}'].join(' '),
                  description: "#{method_to_instrument}",
                  options: args
                }
                ActiveSupport::Notifications.instrument("#{method_type}.ok_hbase", opts) do
                  super
                end
              end
              RUBY
            end
          end

          def batch(timestamp = nil, batch_size = nil, transaction = false)

             batch_wrapper = Proc.new do |*args, &block|
              opts = {
                  name: [table_name, 'write'].join(' '),
                  description: "send_batch",
                  options: args
              }
              ActiveSupport::Notifications.instrument("write.ok_hbase", opts, &block)
            end

            batch = Batch.new(self, timestamp, batch_size, transaction)
            batch.batch_wrapper = batch_wrapper
            batch
          end
        end
      end
    end
  end
end
