# frozen_string_literal: true

require "test_helper"

class ApplicationSystemTestCase < ActionDispatch::SystemTestCase
  driven_by :selenium, using: :headless_chrome, options: { url: "http://chrome:4444/wd/hub" }
end

Capybara.server = :puma, { Silent: true }
Capybara.always_include_port = true
