require 'thor'

require 'ruby-debug'

class RadioactiveBunnies::CLI < Thor
  BUNNIES =<<-EOF

    (\\___/)
    (='.'=)  Radioactive Bunnies!
    (")_(")  JRuby based workers on top of march_hare

  EOF

  desc 'run', "run workers from a file"
  def start_workers(workerfile)

    require workerfile
    # enumerate all workers
    workers = []
    ObjectSpace.each_object(Class){|o| workers << o if o.ancestors.map(&:name).include? "RadioactiveBunnies::Worker"}
    workers.uniq!

    puts BUNNIES

    c = RadioactiveBunnies::Context.new(enable_web_stats: true)
    c.logger.info "Discovered #{workers.inspect}"
    c.run *workers
    Signal.trap('INT') { c.stop; exit! }
  end
end
