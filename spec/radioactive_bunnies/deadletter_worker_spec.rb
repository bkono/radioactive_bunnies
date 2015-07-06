require 'spec_helper'
require 'radioactive_bunnies'
require 'support/workers/timeout_worker'
JOB_TIMEOUT = 1
class DeadletterDefaultWorker
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.default',
    prefetch: 20, durable: false, timeout_job_after: 5, threads: 1, append_env: true,
    exchange: {type: :fanout, name: 'deadletter.exchange'}
  def work(metadata, msg)
    true
  end
end

class DeadletterSecondWorker
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.second',
    prefetch: 20, durable: false, timeout_job_after: 5, threads: 1, append_env: true,
    exchange: {type: :fanout, name: 'deadletter.exchange'}
  def work(metadata, msg)
    true
  end
end

class DeadletterDefaultWorkerTwo
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.default.two',
    prefetch: 20, durable: false, timeout_job_after: 5, threads: 1, append_env: true,
    exchange: {type: :fanout, name: nil}

  def work(metadata, msg)
    true
  end
end

class DeadletterProducer
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.producer',
    prefetch: 20, timeout_job_after: JOB_TIMEOUT, threads: 2,
    deadletter_workers: ['DeadletterDefaultWorker', 'DeadletterSecondWorker']
  def work(metadata, msg)
    sleep (JOB_TIMEOUT + 5)
  end
end

class DeadletterProducerNonArray
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.producer.nonarray',
    prefetch: 20, timeout_job_after: 1, threads: 2,
    deadletter_workers: 'DeadletterDefaultWorker'
  def work(metadata, msg)
    false
  end
end

class DeadletterProducerBroken
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.producer.nonarray',
    prefetch: 20, timeout_job_after: 1, threads: 2,
    deadletter_workers: %w(DeadletterDefaultWorker DeadletterDefaultWorkerTwo)
  def work(metadata, msg)
    false
  end
end

describe RadioactiveBunnies::DeadletterWorker do

  before(:all) do
    @conn = MarchHare.connect
    @ch = @conn.create_channel
    @ctx = RadioactiveBunnies::Context.new
    @ctx.log_with(Logger.new(STDOUT))
    @ctx.run DeadletterDefaultWorker, DeadletterProducer, DeadletterProducerNonArray
    ['deadletter.producer', 'deadletter.producer.nonarray'].each do |r_key|
      @ch.default_exchange.publish("hello world", routing_key: r_key)
    end
    sleep 2
  end

  after(:all) do
    @ctx.stop
    @conn.close
  end
  context 'when a worker has at least one deadletter class' do

    it 'registers with the specified deadletter worker based on a single string class name' do
      expect(DeadletterDefaultWorker.deadletter_producers).to include(DeadletterProducer.name)
    end

    it 'notifies the deadletter worker when a single deadletter work is identified' do
      expect(DeadletterDefaultWorker.deadletter_producers).to include(DeadletterProducerNonArray.name)
    end

    it 'starts any deadletter workers that are not running regardless of class name' do
      expect(DeadletterSecondWorker.running?).to be_truthy
    end

    context 'with two deadletter workers requesting different deadletter exchange names' do
      it 'a worker raises an error on start' do
        expect{DeadletterProducerBroken.start(@ctx)}.
          to raise_error RadioactiveBunnies::DeadletterError
      end
    end

    context 'with a deadletter worker that has a nil exchange name' do
      it 'a worker raises an error on start' do
        DeadletterProducerBroken.queue_opts[:deadletter_workers] = 'DeadletterDefaultWorkerTwo'
        expect{DeadletterProducerBroken.start(@ctx)}.
          to raise_error RadioactiveBunnies::DeadletterError
      end
    end

  end

  describe '.deadletter_queue_config(q_opts)' do
    it 'returns a hash containing an arguments key with the deadletter exchange config' do
      expect(described_class.deadletter_queue_config(DeadletterProducer.queue_opts)).
        to include(arguments: {'x-dead-letter-exchange' => DeadletterDefaultWorker.queue_opts[:exchange][:name]})
    end
  end

  context 'when a worker with deadletter enabled timesout' do
    it 'sends the deadletter to the correct deadletter worker' do
      expect(DeadletterDefaultWorker.jobs_stats[:passed]).to eql 2
    end
  end
end
