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

    def from_queue(q_name, opts={})
      @queue_name = q_name
      @queue_opts = opts
    end

    def start(context)
      @context = context
      startup_init
      @queue = build_queue

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
      say "workers up."
    end

    def stop
      return if stopped?
      say "stopping"
      @thread_pool.shutdown_now
      say "stopped"
      @stopped = true
    end

    def stopped?
      @stopped
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
      deadletter_init(@queue_opts)
      @working_since = Time.now
      @jobs_stats = { :failed => Atomic.new(0), :passed => Atomic.new(0) }
      @logger = @context.logger
      set_thread_pool
    end

    def set_thread_pool
      if @queue_opts[:threads]
        @thread_pool = MarchHare::ThreadPools.fixed_of_size(@queue_opts[:threads])
      else
        @thread_pool = MarchHare::ThreadPools.dynamically_growing
      end
    end

    def build_queue
      @queue_name = "#{@queue_name}_#{@context.opts[:env]}" if @queue_opts[:append_env]
      q = @context.queue_factory.build_queue(@queue_name, @queue_opts)
      say queue_description
      q
    end

    def queue_description
      @description ||= begin
        desc = (@queue_opts[:threads] && "#{@queue_opts[:threads]} threads ") || ''
        desc += "with #{@queue_opts[:prefetch]} prefetch on <#{@queue_name}>."
      end
    end

    def say(text)
      @logger.info "[#{self.name}] #{text}"
    end

    def error(text, metadata)
      @logger.error "[#{self.name}] #{text} <#{metadata.inspect}>"
    end

    def incr!(what)
      @jobs_stats[what].update { |v| v + 1 }
    end
  end
end
