# frozen_string_literal: true

require "cases/helper"

class PostgreSQLPipelineTest < ActiveRecord::PostgreSQLTestCase
  self.use_transactional_tests = false
  def setup
    @connection = ActiveRecord::Base.lease_connection
    @connection.execute("DROP TABLE IF EXISTS pipeline_test_table")
    @connection.execute("CREATE TABLE pipeline_test_table (id SERIAL PRIMARY KEY, name VARCHAR(50))")
  end

  def teardown
    @connection.execute("DROP TABLE IF EXISTS pipeline_test_table")
  end

  def test_pipeline_support_detection
    if @connection.respond_to?(:pipeline_active?)
      # Test that pipeline mode is initially inactive
      assert_not @connection.pipeline_active?
    else
      skip "Pipeline functionality not available"
    end
  end

  def test_basic_pipeline_functionality
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    result = nil
    
    @connection.with_pipeline do
      @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test1')")
      result = @connection.exec_query("SELECT COUNT(*) as count FROM pipeline_test_table")
    end
    
    # Check that the query returns the expected count
    assert_equal 1, result.rows.first.first.to_i
  end

  def test_pipeline_error_wrapping
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # Test that PostgreSQL errors get wrapped in ActiveRecord exceptions
    exception = assert_raises(ActiveRecord::StatementInvalid) do
      @connection.with_pipeline do
        @connection.exec_query("INVALID SQL STATEMENT")
      end
    end
    
    # Verify it's properly wrapped as an ActiveRecord exception
    assert_instance_of ActiveRecord::StatementInvalid, exception
    assert_match(/syntax error/, exception.message.downcase)
  end

  def test_pipeline_error_from_ignored_result
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # Test that errors from queries with ignored results still get raised
    exception = assert_raises(ActiveRecord::StatementInvalid) do
      @connection.with_pipeline do
        # This query has an error but we don't access its result
        @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test1', 'extra_param')")
        # This query is valid - errors should still be raised when pipeline ends
        valid_result = @connection.exec_query("SELECT 1")
        # Don't access the error result, but the error should still be raised
      end
    end
    
    # Verify it's properly wrapped and contains expected error info
    assert_instance_of ActiveRecord::StatementInvalid, exception
    assert_match(/more expressions than target columns/, exception.message.downcase)
  end

  def test_pipeline_error_cleanup_repeated
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # Test that we can handle pipeline errors repeatedly without connection state issues
    2.times do |i|
      exception = assert_raises(ActiveRecord::StatementInvalid) do
        @connection.with_pipeline do
          # This query has an error but we don't access its result
          @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test#{i}', 'extra_param')")
          # This query is valid
          valid_result = @connection.exec_query("SELECT #{i + 1}")
        end
      end
      
      # Verify it's properly wrapped
      assert_instance_of ActiveRecord::StatementInvalid, exception
      assert_match(/more expressions than target columns/, exception.message.downcase)
    end
  end

  def test_pipeline_returns_pipeline_result_objects
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # Execute queries and capture results immediately
    result1 = nil
    result2 = nil
    
    @connection.with_pipeline do
      # Use internal_exec_query to get PipelineResult objects directly
      # exec_query always returns immediate results per the public API contract
      result1 = @connection.send(:internal_exec_query, "SELECT 1 as num", "Test")
      result2 = @connection.send(:internal_exec_query, "SELECT 2 as num", "Test")
      
      # At this point, results should be PipelineResult objects and pending
      assert_instance_of ActiveRecord::PipelineResult, result1
      assert_instance_of ActiveRecord::PipelineResult, result2  
      assert result1.pending?, "First result should be pending"
      assert result2.pending?, "Second result should be pending"
    end
    
    # After pipeline block, results should be resolved
    assert_not result1.pending?, "First result should no longer be pending"
    assert_not result2.pending?, "Second result should no longer be pending"
    assert_equal 1, result1.rows.first.first
    assert_equal 2, result2.rows.first.first
  end

  def test_pipeline_error_propagation
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    bad_result = nil
    good_result = nil
    pipeline_exception = nil
    
    begin
      @connection.with_pipeline do
        # Execute bad query first - in pipeline mode, this returns a PipelineResult
        # In non-pipeline mode, this would raise immediately
        bad_result = @connection.exec_query("INSERT INTO nonexistent_table (col) VALUES ('test')")
        good_result = @connection.exec_query("SELECT 42 as answer")
        
        # Pipeline mode: both results should be PipelineResult objects
        assert_instance_of ActiveRecord::PipelineResult, bad_result
        assert_instance_of ActiveRecord::PipelineResult, good_result
      end
    rescue ActiveRecord::StatementInvalid => e
      pipeline_exception = e
    end
    
    # In pipeline mode, the error should occur during cleanup
    assert pipeline_exception, "Pipeline should have failed during cleanup"
    assert_match(/nonexistent_table/, pipeline_exception.message.downcase)
    
    # Both queries should have returned PipelineResult objects
    assert_instance_of ActiveRecord::PipelineResult, bad_result
    assert_instance_of ActiveRecord::PipelineResult, good_result
  end

  def test_pipeline_bad_query_returns_result_object
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # In pipeline mode, even bad queries return PipelineResult objects
    # In non-pipeline mode, bad queries raise immediately
    bad_result = nil
    
    exception = assert_raises(ActiveRecord::StatementInvalid) do
      @connection.with_pipeline do
        bad_result = @connection.exec_query("INSERT INTO nonexistent_table (col) VALUES ('test')")
        # The bad query should return a PipelineResult object, not raise immediately
        assert_instance_of ActiveRecord::PipelineResult, bad_result
      end
    end
    
    # Pipeline should fail during cleanup, but we got the result object
    assert_instance_of ActiveRecord::PipelineResult, bad_result
    assert_match(/nonexistent_table/, exception.message.downcase)
  end

  def test_pipeline_good_query_after_bad_query_still_gets_result
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # In pipeline mode, queries after bad queries still return result objects
    # In non-pipeline mode, execution would stop at the first bad query
    bad_result = nil
    good_result = nil
    
    exception = assert_raises(ActiveRecord::StatementInvalid) do
      @connection.with_pipeline do
        bad_result = @connection.exec_query("INSERT INTO nonexistent_table (col) VALUES ('test')")
        good_result = @connection.exec_query("SELECT 1")
        
        # Both should be PipelineResult objects in pipeline mode
        assert_instance_of ActiveRecord::PipelineResult, bad_result
        assert_instance_of ActiveRecord::PipelineResult, good_result
      end
    end
    
    # Pipeline failed, but both queries returned result objects
    assert_instance_of ActiveRecord::PipelineResult, bad_result
    assert_instance_of ActiveRecord::PipelineResult, good_result
    assert_match(/nonexistent_table/, exception.message.downcase)
  end

  def test_pipeline_result_access_within_pipeline_block
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # This test demonstrates a bug: accessing PipelineResult data within the pipeline block
    # should work but currently causes issues during pipeline cleanup
    result1 = nil
    result2 = nil
    
    @connection.with_pipeline do
      result1 = @connection.exec_query("SELECT 1 as num")
      result2 = @connection.exec_query("SELECT 2 as num")
      
      # These should work - results should be accessible on demand within the pipeline
      assert_equal 1, result1.rows.first.first
      assert_equal 2, result2.rows.first.first
    end
  end

  def test_pipeline_result_access_with_errors_within_pipeline_block
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # Test accessing error results within pipeline block: errors should be accessible
    # individually, and pipeline cleanup should handle PGRES_PIPELINE_ABORTED properly  
    bad_result = nil
    good_result = nil
    bad_error = nil
    pipeline_exception = nil
    
    begin
      @connection.with_pipeline do
        # Execute bad query first, then good query
        bad_result = @connection.exec_query("INSERT INTO nonexistent_table (col) VALUES ('test')")
        good_result = @connection.exec_query("SELECT 42 as answer")
        
        # In pipeline mode, both results exist as PipelineResult objects
        assert_instance_of ActiveRecord::PipelineResult, bad_result
        assert_instance_of ActiveRecord::PipelineResult, good_result
        
        # Accessing the bad result should raise the query error immediately
        begin
          bad_result.rows
        rescue ActiveRecord::StatementInvalid => e
          bad_error = e
        end
        
        # Don't access good_result - it will have PGRES_PIPELINE_ABORTED status
        # and will be handled during pipeline cleanup
      end
    rescue ActiveRecord::StatementInvalid => e
      pipeline_exception = e
    end
    
    # Should have caught the bad query error when accessing bad_result
    assert bad_error, "Bad query should have raised an error when accessed"
    assert_match(/nonexistent_table/, bad_error.message.downcase)
    
    # Pipeline cleanup should raise error for the aborted good_result  
    assert pipeline_exception, "Pipeline should have failed during cleanup"
    assert_match(/aborted due to an earlier error/, pipeline_exception.message.downcase)
    
    # Both queries should have returned PipelineResult objects
    assert_instance_of ActiveRecord::PipelineResult, bad_result
    assert_instance_of ActiveRecord::PipelineResult, good_result
  end

  def test_pipelined_connection_configuration
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    # Test that connection configuration works properly with pipelining during setup
    # This verifies that the internal pipelining of SET statements doesn't break configuration
    
    # First verify that our current connection (which was set up with pipelining) has correct settings
    conforming_strings_result = @connection.exec_query("SHOW standard_conforming_strings")
    assert_equal "on", conforming_strings_result.rows.first.first
    
    interval_style_result = @connection.exec_query("SHOW intervalstyle")
    assert_equal "iso_8601", interval_style_result.rows.first.first
    
    # Now test the configure_connection pipelining by manually calling it
    # Save current settings
    original_timezone = @connection.exec_query("SHOW timezone").rows.first.first
    original_statement_timeout = @connection.exec_query("SHOW statement_timeout").rows.first.first
    
    # Simulate what happens during connection setup with custom variables
    @connection.instance_variable_set(:@config, @connection.instance_variable_get(:@config).merge(
      variables: {
        'timezone' => 'UTC',
        'statement_timeout' => '45s',
        'lock_timeout' => '15s'
      }
    ))
    
    # Call the configure_connection method that uses pipelining
    @connection.send(:configure_connection)
    
    # Verify all settings were applied correctly via pipelining
    timezone_result = @connection.exec_query("SHOW timezone")
    assert_equal "UTC", timezone_result.rows.first.first
    
    statement_timeout_result = @connection.exec_query("SHOW statement_timeout")
    assert_equal "45s", statement_timeout_result.rows.first.first
    
    lock_timeout_result = @connection.exec_query("SHOW lock_timeout")
    assert_equal "15s", lock_timeout_result.rows.first.first
    
    # Verify standard settings were still applied
    conforming_strings_result = @connection.exec_query("SHOW standard_conforming_strings")
    assert_equal "on", conforming_strings_result.rows.first.first
    
    interval_style_result = @connection.exec_query("SHOW intervalstyle")
    assert_equal "iso_8601", interval_style_result.rows.first.first
  end
end