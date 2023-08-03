module RedisConsumerGroup
  attr_accessor :options, :continue_consuming

  class << self
    attr_accessor :redis_opts

    def configure
      yield self
    end
  end

  def consume
    raise "Invalid Options" unless valid?

    # Need a new redis client per invocation given
    # if it fails mid way the internal buffer isn't reset.
    r = Redis.new(RedisConsumerGroup.redis_opts)
    self.continue_consuming = true
    setup_sig_handler
    
    last_id       = '0-0'
    check_backlog = true

    while continue_consuming? do
      if check_backlog
        id = last_id
      else
        id = '>'
      end

      items = []

      begin
        items = r.xreadgroup(options[:group],
                                 options[:name],
                                 options[:stream],
                                 id,
                                 count: options[:count],
                                 block: options[:block])
      rescue => ex
        # TODO: Determine what bug is where redis server responds or redis-client parses with ':1'
        # this seems to be replicated by aborting the client and starting back up followed by quickly sending new messages
        raise
        puts ex.message
      end

      if items.empty?
        puts "Stream empty"
        next
      end

      # If we receive an empty reply, it means we were consuming our history
      # and that the history is now empty. Let's start to consume new messages.
      check_backlog = !items[options[:stream]].empty?

      items[options[:stream]].each do |id, payload|
        begin
          process_event(id, payload)
        rescue => ex
          # Failed to enqueue
          # Plan failure scenario
          puts "Process items failure: #{ex}"
        end

        last_id = id
      end
      
    end
  end
  
  def ack_event(id)
    # Acknowledge the message as processed
    # Test double acks, idempotent
    # Plan for failure to communicate to redis
    redis.xack(options[:stream], options[:group], id)
  end

  private

  def setup_sig_handler
    Signal.trap("INT") do
      puts "Shutting down"

      self.continue_consuming = false
    end
  end    
  
  def valid?
    !options.empty?
  end

  def continue_consuming?
    self.continue_consuming
  end
end
