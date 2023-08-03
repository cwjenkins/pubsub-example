require "google/cloud/pubsub"

class Events::Adapters::Google
  attr_accessor :pubsub, :subscriber_options
  
  def initialize(adapter_options, subscriber_options)
    self.pubsub = Google::Cloud::PubSub.new(**adapter_options)
    self.subscriber_options = subscriber_options
  end

  def consume
    opts = subscriber_options
    sub = pubsub.subscription(opts[:subscription])

    # Create a subscriber to listen for available messages.
    # By default, this block will be called on 8 concurrent threads
    # but this can be tuned with the `threads` option.
    # The `streams` and `inventory` parameters allow further tuning.
    subscriber = sub.listen threads: { callback: 16 } do |message|
      # process message
      yield message.msg_id, message.data
    end

    subscriber.start
    subscriber
  end

  def acknowledge(message)
    received_message.acknowledge!
  end
end
