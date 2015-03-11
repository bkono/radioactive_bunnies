$:<< File.expand_path('../lib', File.dirname(__FILE__))

require 'rubygems'
require 'radioactive_bunnies'

class FeedWorker
  include RadioactiveBunnies::Worker
  from_queue 'new.feeds', :prefetch => 20, :threads => 13, :durable => true

  def work(msg)
    puts msg
    ack!
  end
end

class FeedDownloader
  include RadioactiveBunnies::Worker
  from_queue 'new.downloads', :durable => true
  def work(msg)
    puts msg
    ack!
  end
end

f = RadioactiveBunnies::Context.new

f.run FeedWorker,FeedDownloader


trap "INT" do
  f.stop
  exit!
end
