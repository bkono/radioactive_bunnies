module RadioactiveBunnies
  module DeadletterWorker
    module ClassMethods
      def register_with_deadletter_worker(worker_class)
        deadletter_producers << worker_class.name
      end

      def deadletter_producers
        @dl_producers ||= []
      end

      def deadletter_init(context, worker_opts)
        return unless !!worker_opts[:deadletter_workers]
        worker_list = deadletter_klasses_for(worker_opts[:deadletter_workers])
        exchanges = deadletter_exchanges_from_workers(worker_list)
        validate_deadletter_exchanges!(exchanges)
        config_and_init_workers(context, worker_list)
      end

      def deadletter_klasses_for(workers)
        [workers].flatten.compact.map do |klass_name|
          RadioactiveBunnies::Ext::Util.constantize!(klass_name)
        end
      end

      def deadletter_exchanges_from_workers(deadletter_workers)
        deadletter_workers.inject([]) do |deadletter_exchange, worker|
          worker.register_with_deadletter_worker(self)
          deadletter_exchange << worker.queue_opts[:exchange][:name]
        end
      end

      def validate_deadletter_exchanges!(exchanges)
        raise DeadletterError.multiple_exchanges(self, exchanges) unless 1 == exchanges.uniq.count
        raise DeadletterError.nil_exchange(self) if exchanges.include? nil
        self.deadletter_exchange = exchanges.first
      end

      def config_and_init_workers(context, worker_list = [])
        worker_list.each do |deadletter_worker|
          unless context.workers.include? deadletter_worker
            context.workers << deadletter_worker
            deadletter_worker.start(context) unless deadletter_worker.running?
          end
          deadletter_worker.add_binding(routing_key)
        end
      end

    end

    def self.deadletter_queue_config(q_opts)
      return {} unless !!q_opts[:deadletter_exchange]
      {arguments: {'x-dead-letter-exchange' => q_opts[:deadletter_exchange]}}
    end
  end

  class DeadletterError < StandardError
    def self.multiple_exchanges(klass, exchanges)
      new("Multiple deadletter exchanges in [#{klass.name}] - exchanges are <#{exchanges}>")
    end
    def self.nil_exchange(klass)
      new("Found nil for deadletter exchange name in [#{klass.name}]")
    end
  end
end
