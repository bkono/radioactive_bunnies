require 'atomic'
require 'radioactive_bunnies/context'
require 'radioactive_bunnies/deadletter_worker'
module RadioactiveBunnies::Worker
  def ack!
    true
  end

  def work; end

  def self.included(base)
    base.extend ClassMethods
    base.extend RadioactiveBunnies::DeadletterWorker::ClassMethods
    RadioactiveBunnies::Context.add_worker(base)
  end

  module ClassMethods

    def routing_key
      @queue_opts[:routing_key] || @queue_name
    end

    def from_queue(q_name, opts={})
      @queue_name = q_name
      @queue_opts = opts
    end

    def add_binding(route_key)
      @queue.bind(@exchange, routing_key: route_key)
    end

    def start(context)
      return if running? # don't call me twice

      @context = context
      startup_init
      @exchange, @queue = build_exchange_and_queue

      @queue.subscribe(:ack => true, :blocking => false, :executor => @thread_pool) do |metadata, payload|
        wkr = new
        begin
          Timeout::timeout(@queue_opts[:timeout_job_after]) do
            if(wkr.work(metadata, payload))
              metadata.ack
              incr! :passed
            else
              metadata.reject
              incr! :failed
              error "REJECTED", metadata
            end
          end
        rescue Timeout::Error
          metadata.reject
          incr! :failed
          error "TIMEOUT #{@queue_opts[:timeout_job_after]}s", metadata
        rescue
          metadata.reject
          incr! :failed
          error "ERROR #{$!}", metadata
        end
      end
      say queue_description + "\nworkers up."
    end

    def stop
      return if stopped?
      @thread_pool.shutdown_now
      say "stopped"
      @running = false
    rescue StandardError => e
      @running = false
      logger.error "An error occurred while stopping worker #{e.message}"
    end

    def stopped?
      !@running
    end

    def running?
      @running
    end

    def queue_opts
      @queue_opts
    end

    def queue_name
      @queue_name
    end

    def deadletter_exchange=(exchange)
      @queue_opts[:deadletter_exchange] = exchange
    end

    def jobs_stats
      Hash[ @jobs_stats.map{ |k,v| [k, v.value] } ].merge({ :since => @working_since.to_i })
    end

    private

    def startup_init
      @running = true
      @queue_name = "#{@queue_name}_#{@context.opts[:env]}" if @queue_opts[:append_env]
      deadletter_init(@context, @queue_opts)
      @working_since = Time.now
      @jobs_stats = { :failed => Atomic.new(0), :passed => Atomic.new(0) }
      set_thread_pool
    end

    def set_thread_pool
      if @queue_opts[:threads]
        @thread_pool = MarchHare::ThreadPools.fixed_of_size(@queue_opts[:threads])
      else
        @thread_pool = MarchHare::ThreadPools.dynamically_growing
      end
    end

    def build_exchange_and_queue
      RadioactiveBunnies::QueueFactory.build_exchange_and_queue(@context, @queue_name, @queue_opts)
    end

    def queue_description
      @description ||= begin
        desc = (@queue_opts[:threads] && "#{@queue_opts[:threads]} threads ") || ''
        desc += "with #{@queue_opts[:prefetch]} prefetch on <#{@queue_name}>."
      end
    end

    def logger
      @logger ||= @context.logger
    end

    def say(text)
      logger.info "[#{self.name}] #{text}"
    end

    def error(text, metadata)
      logger.error "[#{self.name}] #{text} <#{metadata.inspect}>"
    end

    def incr!(what)
      @jobs_stats[what].update { |v| v + 1 }
    end
  end
end
