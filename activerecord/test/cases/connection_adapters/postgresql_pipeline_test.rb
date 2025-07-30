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
      result1 = @connection.exec_query("SELECT 1 as num")
      result2 = @connection.exec_query("SELECT 2 as num") 
      
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

  def test_pipeline_error_propagation_vs_individual_query_handling
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    bad_result = nil
    good_result = nil
    bad_error = nil
    pipeline_exception = nil
    
    begin
      @connection.with_pipeline do
        # Execute bad query first, rescue individual query errors
        begin
          bad_result = @connection.exec_query("INSERT INTO nonexistent_table (col) VALUES ('test')")
        rescue ActiveRecord::StatementInvalid => e
          bad_error = e
        end
        
        # Execute good query - in pipeline mode this still gets a result object,
        # in non-pipeline mode this executes normally after rescuing the first error
        good_result = @connection.exec_query("SELECT 42 as answer")
        
        # In pipeline mode, both results should be PipelineResult objects
        # In non-pipeline mode, bad_result is nil (error was rescued) and good_result is a normal Result
        if bad_result
          assert_instance_of ActiveRecord::PipelineResult, bad_result
          assert_instance_of ActiveRecord::PipelineResult, good_result
        end
      end
    rescue ActiveRecord::StatementInvalid => e
      # This should only happen in pipeline mode during cleanup
      pipeline_exception = e
    end
    
    # Test the behavioral differences
    if bad_result && good_result.is_a?(ActiveRecord::PipelineResult)
      # Pipeline mode: both results exist as PipelineResult objects
      assert_instance_of ActiveRecord::PipelineResult, bad_result  
      assert_instance_of ActiveRecord::PipelineResult, good_result
      assert pipeline_exception, "Pipeline should have failed during cleanup"
      assert_match(/nonexistent_table/, pipeline_exception.message.downcase)
    else
      # Non-pipeline mode: bad query raised immediately, good query succeeded
      assert bad_error, "Bad query should have raised an error"
      assert_match(/nonexistent_table/, bad_error.message.downcase)
      assert_nil bad_result, "Bad result should be nil after rescue"
      assert good_result, "Good query should have succeeded"
      assert_equal 42, good_result.rows.first.first
    end
  end
end