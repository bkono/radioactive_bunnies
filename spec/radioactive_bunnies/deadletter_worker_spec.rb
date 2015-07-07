require 'spec_helper'
require 'radioactive_bunnies'
require 'support/workers/deadletter_workers'

PRODUCER_ITERATIONS = 10

describe RadioactiveBunnies::DeadletterWorker do

  before(:all) do
    @conn = MarchHare.connect
    @ch = @conn.create_channel
    @ctx = RadioactiveBunnies::Context.new
    @ctx.log_with(Logger.new(STDOUT))
    @ctx.run DeadletterProducer, DeadletterProducerNonArray
    @producer_queues = ['deadletter.producer', 'deadletter.producer.nonarray']
    PRODUCER_ITERATIONS.times do
      @producer_queues.each do |r_key|
        @ch.default_exchange.publish("hello world", routing_key: r_key)
      end
    end
    sleep 5
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

    before do
      DeadletterProducerBroken.stop if DeadletterProducerBroken.running?
    end

    context 'with two deadletter workers requesting different deadletter exchange names' do
      it 'a worker raises an error on start' do
        expect{DeadletterProducerBroken.start(@ctx)}.
          to raise_error RadioactiveBunnies::DeadletterError
      end
    end

    context 'with a deadletter worker that has a nil exchange name' do
      it 'a worker raises an error on start' do
        original_workers = DeadletterProducerBroken.queue_opts[:deadletter_workers]
        DeadletterProducerBroken.queue_opts[:deadletter_workers] = 'DeadletterDefaultWorkerTwo'
        expect{DeadletterProducerBroken.start(@ctx)}.
          to raise_error RadioactiveBunnies::DeadletterError
        DeadletterProducerBroken.queue_opts[:deadletter_workers] = original_workers
      end
    end

  end

  describe '.deadletter_queue_config(q_opts)' do
    it 'returns a hash containing an arguments key with the deadletter exchange config' do
      expect(described_class.deadletter_queue_config(DeadletterProducer.queue_opts)).
        to include(arguments: {'x-dead-letter-exchange' => DeadletterDefaultWorker.queue_opts[:exchange][:name]})
    end
  end

  context 'when a worker with deadletter enabled rejects a message' do
    let(:total_messages_sent) {PRODUCER_ITERATIONS * @producer_queues.size}

    it 'sends the deadletter to the correct round robin worker' do
      half_of_messages_sent = total_messages_sent / 2
      expect(DeadletterDefaultWorker.jobs_stats[:passed]).to be_within(5).of half_of_messages_sent
      expect(DeadletterSecondWorker.jobs_stats[:passed]).to be_within(5).of half_of_messages_sent
      expect(DeadletterDefaultWorker.jobs_stats[:passed] +
             DeadletterSecondWorker.jobs_stats[:passed]).to eql total_messages_sent
    end
    it 'sends the message to a specific deadletter worker' do
      expect(DeadletterProveWorker.jobs_stats[:passed]).to eql PRODUCER_ITERATIONS
    end
  end
end
