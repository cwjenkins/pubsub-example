class RubyEventConsumer
  include ::Events::Subscriber

  options subscription: 'events'

  # Sleep greater than block to test
  # redis ->
  #  consumer -> BG Job -> POST -> Server to ACK -> redis
  def process_event(id, payload)
    # Enqueue for faktory
    Rails.logger.info "Processing ID: #{id} with payload: #{payload}"

    # Note, acknowledgements need to occur on a separate Redis client
    # Redis server uses a buffer for each client to apply responses and if a client
    # disconnects midway then the next request will retrieve that buffer
    # which may be in a response for a different request (xack vs xreadgroup)
    # ack_event(id)
  end

end
