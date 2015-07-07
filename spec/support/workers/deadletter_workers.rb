require 'radioactive_bunnies'
class DeadletterDefaultWorker
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.default',
    prefetch: 1, durable: false, timeout_job_after: 5, threads: 1,
    exchange: {name: 'deadletter.exchange'}
  def work(metadata, msg)
    ack!
  end
end

class DeadletterProveWorker
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.prove',
    prefetch: 1, durable: false, timeout_job_after: 5, threads: 1,
    exchange: {name: 'deadletter.exchange'}
  def work(metadata, msg)
    ack!
  end
end

class DeadletterSecondWorker
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.default',
    prefetch: 1, durable: false, timeout_job_after: 5, threads: 1,
    exchange: {name: 'deadletter.exchange'}
  def work(metadata, msg)
    ack!
  end
end

class DeadletterDefaultWorkerTwo
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.default.two',
    prefetch: 20, durable: false, timeout_job_after: 5, threads: 1, append_env: true,
    exchange: {name: nil}
  def work(metadata, msg)
    ack!
  end
end

class DeadletterProducer
  include RadioactiveBunnies::Worker
  from_queue 'deadletter.producer',
    prefetch: 20, timeout_job_after: 5, threads: 2,
    deadletter_workers: ['DeadletterDefaultWorker', 'DeadletterSecondWorker', 'DeadletterProveWorker']
  def work(metadata, msg)
    !ack!
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

