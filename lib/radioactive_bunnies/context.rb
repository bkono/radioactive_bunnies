require 'logger'
require 'radioactive_bunnies/web'
require 'thread_safe'
require 'radioactive_bunnies/ext/util'
class RadioactiveBunnies::Context
  attr_reader :opts, :connection, :workers
  OPTS = [:uri, :host, :vhost, :heartbeat, :web_host, :web_port, :web_threadfilter, :env,
          :username, :password, :exchange, :workers_scope]

  EXCHANGE_DEFAULTS = {name: 'frenzy_bunnies', type: :direct, durable: false}.freeze
  CONFIG_DEFAULTS = { host: 'localhost', heartbeat: 30, web_host: 'localhost', web_port: 11333,
    enable_web_stats: false, web_threadfilter: /^pool-.*/, env: 'development',
    exchange: EXCHANGE_DEFAULTS
  }.freeze

  @@known_workers = ThreadSafe::Array.new

  OPTS.each do |option|
    define_method option do |value|
      @opts[option] = value
    end
  end

  def logger
    @logger ||= Logger.new(STDOUT)
  end

  def log_with good_logger
    @opts[:log_with] = good_logger
    @logger = good_logger
  end

  def self.add_worker(wrk_class)
    return @@known_workers unless(!!wrk_class.name rescue nil)
    @@known_workers << wrk_class.name
  end

  def initialize(opts = {})
    @opts = CONFIG_DEFAULTS.merge(opts)
    @env = @opts[:env]
    @logger = @opts[:log_with]
  end

  def default_exchange
    @opts[:exchange]
  end

  def reset_to_default_config
    @opts = {}.merge(CONFIG_DEFAULTS)
  end

  def run(*klasses)
    @workers = (klasses + worker_classes_for_scope).flatten.compact
    start_rabbit_connection!
    @workers.each{|klass| klass.start(self)}
    start_web_console
  end

  def stop
    return if (@connection.nil? || @connection.closed?)
    @logger.info 'Shutting down workers and closing connection'
    stop_workers
    @connection.close
  end

  def workers
    @workers ||= []
  end

  def stop_workers
    return unless !!@workers
    @logger.info 'Stopping workers'
    @workers.each{|klass| klass.stop }
    @logger.info 'Workers have been told to stop'
  end

  def worker_classes_for_scope
    worker_scope = @opts[:workers_scope]
    return [] if worker_scope.to_s.empty?
    @@known_workers.map do |klass_name|
      RadioactiveBunnies::Ext::Util.constantize!(klass_name) if klass_name.start_with? worker_scope
    end
  end

  def rabbit_params
    params = { :heartbeat_interval => @opts[:heartbeat]}
    if !!@opts[:uri]
      params[:uri] = @opts[:uri]
    else
      params.merge!(@opts.select {|k,v| [:host, :username, :password, :vhost, :port].include? k})
    end
    params
  end

  private

  def start_web_console
    return nil unless @opts[:enable_web_stats]
    Thread.new do
      RadioactiveBunnies::Web.run_with(@workers, :host => @opts[:web_host], port: @opts[:web_port],
                                  threadfilter: @opts[:web_threadfilter], logger: @logger)
    end
  end

  def start_rabbit_connection!
    params = rabbit_params
    @connection = MarchHare.connect(params)
    @connection.on_shutdown do |conn, cause|
      @logger.error("Disconnected: #{cause}") unless cause.initiated_by_application?
      stop
    end
  end
end
