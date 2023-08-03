class Events::Adapters::Redis
  attr_accessor :redis_options, :consumer_options, :continue_consuming
  
  def initialize(redis_opts, consumer_opts)
    self.redis_options = redis_opts
    self.consumer_options = consumer_opts
  end
  
  def consume
    # Need a new redis client per invocation given
    # if it fails mid way the internal buffer isn't reset.
    r = Redis.new(redis_options)
    self.continue_consuming = true
    setup_sig_handler

    # First time starting so begin at the head
    last_id       = '0-0'
    check_backlog = true
    retries = 0

    opts = consumer_options
    while continue_consuming? do
      # If we haven't hit the tail keep checking backlog
      if check_backlog
        id = last_id
      else
        id = '>'
      end

      items = []

      begin
        items = r.xreadgroup(opts[:group],
                             opts[:name],
                             opts[:stream],
                             id,
                             count: opts[:count],
                             block: opts[:block])

      rescue Redis::TimeoutError => ex
        # Meh, sometimes you got to shake the line a little
        if retries < 3
          retries += 1
          sleep(retries)
          retry
        else
          # Line appears to be disconnected
          raise
        end
      end
      retries = 0
     
      if items.empty?
        puts "Stream empty"
        next
      end

      # If we receive an empty reply, it means we were consuming our history
      # and that the history is now empty. Let's start to consume new messages.
      check_backlog = !items[opts[:stream]].empty?

      items[opts[:stream]].each do |id, payload|
        begin
          yield id, payload
        rescue => ex
          # Failed to enqueue
          # Plan failure scenario
          puts "Process items failure: #{ex}"
        end

        last_id = id
      end

    end
  end

  def setup_sig_handler
    Signal.trap("INT") do
      puts "Shutting down"

      self.continue_consuming = false
    end
  end

  def continue_consuming?
    continue_consuming
  end
end
