# frozen_string_literal: true

module ActiveSupport
  if RUBY_ENGINE == "ruby"
    class WeakKeyMap < ObjectSpace::WeakKeyMap # :nodoc:
    end
  else
    class WeakKeyMap # :nodoc:
      def initialize
        @map = ObjectSpace::WeakKeyMap.new
        @mutex = Mutex.new
      end

      def [](key)
        @mutex.synchronize { @map[key] }
      end

      def []=(key, value)
        @mutex.synchronize { @map[key] = value }
      end

      def key?(key)
        @mutex.synchronize { @map.key?(key) }
      end

      def delete(key)
        @mutex.synchronize { @map.delete(key) }
      end

      def clear
        @mutex.synchronize { @map.clear }
        self
      end
    end
  end

  class WeakKeyMap # :nodoc:
    # A nil value is a miss. Concurrent callers may compute different values.
    def fetch_or_store(key)
      value = self[key]
      return value unless value.nil?
      value = yield
      stored = self[key]
      stored.nil? ? self[key] = value : stored
    end
  end
end
