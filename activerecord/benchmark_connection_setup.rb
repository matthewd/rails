#!/usr/bin/env ruby
# frozen_string_literal: true

# Benchmark script to measure Active Record connection setup performance
# Run with: bundle exec ruby benchmark_connection_setup.rb
# Run with pipelining disabled: DISABLE_PIPELINE=1 bundle exec ruby benchmark_connection_setup.rb

require "active_record"
require "benchmark"

# Configure database connection similar to test setup
ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  database: "activerecord_unittest",
  encoding: "unicode", 
  variables: {
    timezone: "UTC",
    statement_timeout: "30s", 
    lock_timeout: "10s",
    idle_in_transaction_session_timeout: "60s",
    log_statement: "none",
    log_min_duration_statement: "1000ms",
    work_mem: "16MB",
    random_page_cost: "1.5"
  }
)

# Get the connection pool for reuse
connection_pool = ActiveRecord::Base.connection_pool

puts "Benchmarking Active Record connection setup..."
puts "Pipeline support: #{ENV['DISABLE_PIPELINE'] == '1' ? 'DISABLED' : 'ENABLED'}"
puts "Connection pool size: #{connection_pool.size}"
puts

# Benchmark connection setup and basic operation
iterations = 500
puts "Running #{iterations} iterations..."
puts

time = Benchmark.realtime do
  iterations.times do
    # Get a new connection from the pool (this triggers configure_connection for new connections)
    conn = connection_pool.checkout
    
    # Perform a basic operation to ensure connection is fully set up
    conn.get_database_version
    
    # Remove connection to force new connection next time (no checkin needed)
    connection_pool.remove(conn)
  end
end

puts "Results:"
puts "=========="
puts "Total time: #{time.round(4)} seconds"
puts "Time per connection: #{(time / iterations * 1000).round(4)} ms"
puts "Connections per second: #{(iterations / time).round(2)}"

puts
puts "Benchmark completed!"
puts "Pipeline support was: #{ENV['DISABLE_PIPELINE'] == '1' ? 'DISABLED' : 'ENABLED'}"