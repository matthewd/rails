# frozen_string_literal: true

require "monitor"

module ActiveSupport
  module Concurrency
    # A monitor that will permit dependency loading while blocked waiting for
    # the lock.
    LoadInterlockAwareMonitor = Monitor
  end
end
