# frozen_string_literal: true

if ENV["BUILDKITE"]
  require "minitest-ci"

  Minitest::Ci.report_dir = File.join(__dir__, "../test-reports/#{ENV['BUILDKITE_JOB_ID']}")

  # allow sending request to Test Analytics
  if defined?(WebMock)
    WebMock.disable_net_connect!(allow: "analytics-api.buildkite.com")
  end
  ENV["BUILDKITE_ANALYTICS_EXECUTION_NAME_PREFIX"] = ENV["BUILDKITE_LABEL"]

  require "buildkite/test_collector"
  require "fileutils"
  FileUtils.mkdir_p("../log") if !File.exist?("../log")
  Buildkite::TestCollector.logger = Buildkite::TestCollector::Logger.new("../log/buildkite-analytics.log")
  Buildkite::TestCollector.configure(hook: :minitest, token: ENV["BUILDKITE_ANALYTICS_TOKEN"])
end
