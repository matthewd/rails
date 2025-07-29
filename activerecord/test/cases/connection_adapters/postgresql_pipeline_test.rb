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
      @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test1', ?)")
      result = @connection.exec_query("SELECT COUNT(*) as count FROM pipeline_test_table")
    end
    
    # Check that the query returns the expected count
    assert_equal 1, result.rows.first.first.to_i
  end

  def test_pipeline_with_binds
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    result = nil
    
    @connection.with_pipeline do
      @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ($1)", "SQL", ["bound_value"])
      result = @connection.exec_query("SELECT name FROM pipeline_test_table WHERE name = $1", "SQL", ["bound_value"])
    end

    assert_equal 1, result.rows.length
    assert_equal "bound_value", result.rows.first.first
  end

  def test_pipeline_result_behavior
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    result = nil
    
    @connection.with_pipeline do
      @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test')")
      result = @connection.exec_query("SELECT COUNT(*) as count FROM pipeline_test_table")
    end
    
    # Test that result behaves like a normal ActiveRecord::Result
    assert_respond_to result, :rows
    assert_respond_to result, :columns
    assert_respond_to result, :empty?
    assert_not result.empty?
    assert_equal 1, result.rows.first.first.to_i
  end

  def test_multiple_queries_in_pipeline
    skip "Pipeline functionality not available" unless @connection.respond_to?(:with_pipeline)
    
    results = []
    
    @connection.with_pipeline do
      results << @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test1')")
      results << @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test2')")
      results << @connection.exec_query("INSERT INTO pipeline_test_table (name) VALUES ('test3')")
      results << @connection.exec_query("SELECT COUNT(*) as count FROM pipeline_test_table")
    end
    
    # Verify that we get proper results
    assert_equal 4, results.length
    
    # Check that the last query returns the expected count
    count_result = results.last
    assert_equal 3, count_result.rows.first.first.to_i
  end
end
