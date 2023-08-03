module Events::Subscriber
  DEFAULT_ADAPTER = :redis

  class << self
    attr_accessor :adapter_klass, :adapter_options

    def configure
      yield self
    end

    module ClassMethods
      def options(options)
        validate_options!(options)

        self.subscriber_options.merge!(options)
      end

      def validate_options!(options)
        raise ArgumentError "Invalid options" unless !options.empty?
      end
    end

    def included(base)
      base.singleton_class.instance_eval do
        attr_accessor :adapter_klass, :adapter_options, :subscriber_options
      end

      base.adapter_klass = adapter_klass || DEFAULT_ADAPTER      
      base.adapter_options = adapter_options || {}
      base.subscriber_options = {}

      base.extend ClassMethods
    end
  end

  attr_accessor :adapter

  def initialize(consumer_name = nil)
    self.class.subscriber_options[:name] = consumer_name unless consumer_name.nil?
  end
  
  def consume
    adapter.consume do |id, payload|
      process_event(id, payload)
    end
  end

  private

  def adapter
    @adapter ||= ::Events::Adapters.const_get(self.class.adapter_klass.capitalize).new(self.class.adapter_options, self.class.subscriber_options)
  end
end
