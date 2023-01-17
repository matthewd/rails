# frozen_string_literal: true

require "test_helper"

class ApplicationSystemTestCase < ActionDispatch::SystemTestCase
  driven_by :selenium, using: :headless_chrome, options: { url: ENV["SELENIUM_DRIVER_URL"] }
end

Capybara.server = :puma, { Silent: true }
Capybara.always_include_port = true
