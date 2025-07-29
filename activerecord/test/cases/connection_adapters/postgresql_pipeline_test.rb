# frozen_string_literal: true

require "cases/helper"

class PostgreSQLPipelineTest < ActiveRecord::PostgreSQLTestCase
  def setup
    @connection = ActiveRecord::Base.connection
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
end