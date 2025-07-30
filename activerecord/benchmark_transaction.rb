#!/usr/bin/env ruby
# frozen_string_literal: true

# Benchmark script to measure Active Record transaction pipelining performance
# Run with: bundle exec ruby benchmark_transaction.rb
# Run with pipelining disabled: DISABLE_PIPELINE=1 bundle exec ruby benchmark_transaction.rb

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

# Get a persistent connection for the benchmark
conn = ActiveRecord::Base.connection

puts "Benchmarking Active Record transaction pipelining..."
puts "Pipeline support: #{ENV['DISABLE_PIPELINE'] == '1' ? 'DISABLED' : 'ENABLED'}"
puts "Using persistent connection: #{conn.class.name}"
puts

# Benchmark transaction operations with nested transactions
iterations = ENV['ITERATIONS']&.to_i || 200
puts "Running #{iterations} iterations..."
puts

time = Benchmark.realtime do
  iterations.times do
    # Perform nested transaction operations to test transaction pipelining
    result = conn.transaction do
      # First query in outer transaction - should pipeline with BEGIN if pipelining enabled
      first_result = conn.select_value("SELECT 1")
      
      # Nested transaction with requires_new (creates savepoint)  
      nested_result = conn.transaction(requires_new: true) do
        # Query in nested transaction - should pipeline with SAVEPOINT if pipelining enabled
        conn.select_value("SELECT 2")
      end
      
      # Final query in outer transaction
      final_result = conn.select_value("SELECT 4")
      
      # Verify the calculation: 1 + 2 + 4 = 7
      # The pipeline results should be transparently resolved to actual values
      total = first_result + nested_result + final_result
      raise "Calculation failed: #{total} != 7 (got #{first_result}, #{nested_result}, #{final_result})" unless total == 7
      
      total
    end
    
    # Verify result
    raise "Transaction result incorrect: #{result} != 7" unless result == 7
  end
end

puts "Results:"
puts "=========="
puts "Total time: #{time.round(4)} seconds"
puts "Time per transaction: #{(time / iterations * 1000).round(4)} ms"
puts "Transactions per second: #{(iterations / time).round(2)}"

puts
puts "Benchmark completed!"
puts "Pipeline support was: #{ENV['DISABLE_PIPELINE'] == '1' ? 'DISABLED' : 'ENABLED'}"