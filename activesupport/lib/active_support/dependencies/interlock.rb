# frozen_string_literal: true

require "active_support/concurrency/share_lock"

module ActiveSupport # :nodoc:
  module Dependencies # :nodoc:
    class Interlock
      def initialize # :nodoc:
        @lock = Concurrent::ReentrantReadWriteLock.new
      end

      def loading(&block)
        yield
      end

      def unloading(&block)
        @lock.with_write_lock(&block)
      end

      def start_unloading
        @lock.acquire_write_lock
      end

      def done_unloading
        @lock.release_write_lock
      end

      def start_running
        @lock.acquire_read_lock
      end

      def done_running
        @lock.release_read_lock
      end

      def running(&block)
        @lock.with_read_lock(&block)
      end

      def permit_concurrent_loads(&block)
        yield
      end

      def raw_state(&block) # :nodoc:
        @lock.raw_state(&block)
      end
    end
  end
end
