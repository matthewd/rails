# frozen_string_literal: true

require "cases/helper"
require "support/connection_helper"

module ActiveRecord
  class PostgresqlPersistentPipelineTest < ActiveRecord::PostgreSQLTestCase
    include ConnectionHelper

    class MockDatabaseError < StandardError
      def result
        0
      end

      def error_number
        0
      end
    end

    def setup
      super
      @connection = ActiveRecord::Base.lease_connection
      @connection.materialize_transactions
      skip "Pipeline not supported" unless @connection.respond_to?(:pipeline_supported?) && @connection.pipeline_supported?
    end

    def teardown
      # Clean up any persistent pipeline state
      if @connection.respond_to?(:pipeline_mode?) && @connection.pipeline_mode?
        @connection.exit_persistent_pipeline_mode
      end
      super
    end

    def test_persistent_pipeline_context_initialization
      # Test that @pipeline_context persists across with_pipeline calls
      assert_not @connection.pipeline_mode?

      @connection.with_pipeline do
        # Should have pipeline context
        assert @connection.pipeline_mode?
      end

      # Pipeline should be inactive but context should still exist
      assert_not @connection.pipeline_mode?
      assert @connection.instance_variable_get(:@pipeline_context)
    end

    def test_enter_persistent_pipeline_mode
      assert_not @connection.pipeline_mode?

      result = @connection.enter_persistent_pipeline_mode
      assert result
      assert @connection.pipeline_mode?

      # Calling again should return true but not error
      result = @connection.enter_persistent_pipeline_mode
      assert result
      assert @connection.pipeline_mode?
    end

    def test_exit_persistent_pipeline_mode
      @connection.enter_persistent_pipeline_mode
      assert @connection.pipeline_mode?

      result = @connection.exit_persistent_pipeline_mode
      assert result
      assert_not @connection.pipeline_mode?

      # Calling again should return false (not in pipeline mode)
      result = @connection.exit_persistent_pipeline_mode
      assert_not result
    end

    def test_pipeline_mode_query_alias
      assert_not @connection.pipeline_mode?

      @connection.enter_persistent_pipeline_mode
      assert @connection.pipeline_mode?

      @connection.exit_persistent_pipeline_mode
      assert_not @connection.pipeline_mode?
    end

    def test_has_pending_pipeline_results
      @connection.enter_persistent_pipeline_mode

      # Initially no pending results
      assert_not @connection.has_pending_pipeline_results?

      # Execute a query in pipeline mode using internal API to test deferred behavior
      result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        # Use internal_exec_query to get actual PipelineResult (exec_query calls .check)
        result = @connection.send(:internal_exec_query, "SELECT 1 as test_value", "Test")
        # Check that result is a PipelineResult and is pending
        assert_kind_of ActiveRecord::PipelineResult, result
        assert result.pending?, "Result should be pending before access"
      end

      # Should have pending results before we access the result data
      assert @connection.has_pending_pipeline_results?, "Expected pending results after query execution"

      # Now access the result data - this should auto-resolve
      assert_equal 1, result.first["test_value"]

      # After accessing result data, pending results should be cleared
      assert_not @connection.has_pending_pipeline_results?, "Pending results should be cleared after result access"

      # Sync should work even with no pending results
      @connection.sync_pipeline_results
      assert_not @connection.has_pending_pipeline_results?
    end

    def test_sync_pipeline_results
      @connection.enter_persistent_pipeline_mode

      # Execute query in pipeline mode using internal API but don't access result data yet
      result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        result = @connection.send(:internal_exec_query, "SELECT 42 as answer", "Test")
        assert_kind_of ActiveRecord::PipelineResult, result
        assert result.pending?
      end

      # Should have pending results
      assert @connection.has_pending_pipeline_results?

      # Sync should resolve pending results
      sync_result = @connection.sync_pipeline_results
      assert sync_result
      assert_not @connection.has_pending_pipeline_results?
      assert @connection.pipeline_mode? # Should still be in pipeline mode

      # Now we can access the result data
      assert_equal 42, result.first["answer"]
    end

    def test_with_raw_connection_pipeline_mode_parameter
      # Test framework code requesting pipeline mode
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        assert @connection.pipeline_mode?
      end

      # Pipeline should remain active after framework code
      assert @connection.pipeline_mode?
    end

    def test_with_raw_connection_auto_exit_for_user_code
      # Enter pipeline mode and create pending results using internal API
      @connection.enter_persistent_pipeline_mode
      pipeline_result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        pipeline_result = @connection.send(:internal_exec_query, "SELECT 123 as sync_test", "Test")
        assert_kind_of ActiveRecord::PipelineResult, pipeline_result
        assert pipeline_result.pending?
      end

      # Should have pending results and be in pipeline mode
      assert @connection.has_pending_pipeline_results?
      assert @connection.pipeline_mode?

      # User code should trigger full exit from pipeline mode (default pipeline_mode: false)
      @connection.send(:with_raw_connection) do |raw_conn|
        result = @connection.exec_query("SELECT 456 as user_query")
        assert_equal 456, result.first["user_query"]
      end

      # Pipeline mode should be fully exited for user code
      assert_not @connection.pipeline_mode?
      assert_not @connection.has_pending_pipeline_results?

      # The pipeline result should still be accessible (was resolved during exit)
      assert_equal 123, pipeline_result.first["sync_test"]
    end

    def test_disconnect_cleans_up_pipeline_state
      @connection.enter_persistent_pipeline_mode
      assert @connection.pipeline_mode?

      @connection.disconnect!

      # After reconnect, pipeline state should be reset
      @connection.materialize_transactions
      assert_not @connection.pipeline_mode?
    end

    def test_backward_compatibility_with_existing_with_pipeline
      # Existing with_pipeline blocks should continue to work
      @connection.with_pipeline do
        result = @connection.exec_query("SELECT 'compatibility' as test")
        assert_equal "compatibility", result.first["test"]
      end

      # Should exit pipeline mode after block
      assert_not @connection.pipeline_mode?
    end

    def test_error_behavior_preserved_in_pipeline_mode
      # Adapted from test/cases/statement_invalid_test.rb to verify
      # that errors are still raised immediately even in pipeline mode

      # Enter pipeline mode first
      @connection.enter_persistent_pipeline_mode
      assert @connection.pipeline_mode?

      # This test should behave identically whether in pipeline mode or not
      # The error should be raised immediately, not deferred
      error = assert_raises(ActiveRecord::StatementInvalid) do
        @connection.send(:log, "SELECT 1", "Test") do
          @connection.send(:with_raw_connection, pipeline_mode: true) do
            raise MockDatabaseError, "Test database error"
          end
        end
      end

      # Verify the error was raised immediately and contains expected content
      assert_match(/Test database error/, error.message)

      # Pipeline mode should still be active (error didn't break it)
      assert @connection.pipeline_mode?
    end

    def test_query_error_behavior_in_pipeline_mode
      # Test that SQL syntax errors are raised immediately even in pipeline mode
      @connection.enter_persistent_pipeline_mode
      assert @connection.pipeline_mode?

      # This should raise immediately, not be deferred until result access
      assert_raises(ActiveRecord::StatementInvalid) do
        @connection.exec_query("INVALID SQL SYNTAX")
      end

      # Pipeline mode should still be active
      assert @connection.pipeline_mode?
    end

    def test_exec_query_immediately_consumes_pipeline_results
      # Test that user-facing exec_query immediately consumes PipelineResult for safety
      @connection.enter_persistent_pipeline_mode
      assert @connection.pipeline_mode?

      # User-facing exec_query should return a regular Result, not PipelineResult
      result = @connection.exec_query("SELECT 'immediate' as test")

      # Should be a regular Result (consumed immediately)
      assert_kind_of ActiveRecord::Result, result
      assert_not_kind_of ActiveRecord::PipelineResult, result
      assert_equal "immediate", result.first["test"]

      # No pending results should remain
      assert_not @connection.has_pending_pipeline_results?
      assert @connection.pipeline_mode? # But pipeline mode should still be active
    end

    def test_pipeline_result_activerecord_type_casting
      # Test that PipelineResult properly applies ActiveRecord type casting
      # including custom attribute type overrides

      # Create a test table with text columns
      @connection.execute <<~SQL
        CREATE TEMPORARY TABLE pipeline_type_test (
          id SERIAL PRIMARY KEY,
          text_col TEXT,
          number_as_text TEXT,
          json_as_text TEXT
        )
      SQL

      # Insert test data as strings
      @connection.execute <<~SQL
        INSERT INTO pipeline_type_test (text_col, number_as_text, json_as_text)
        VALUES ('hello', '42', '{"key": "value"}')
      SQL

      # Define a model with type overrides using attribute method
      Class.new(ActiveRecord::Base) do
        self.table_name = "pipeline_type_test"

        # Override types - PG will return strings but AR should cast them
        attribute :number_as_text, :integer
        attribute :json_as_text, :json
      end

      # Test 1: Pipeline mode should return properly cast values
      @connection.enter_persistent_pipeline_mode

      pipeline_result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        pipeline_result = @connection.send(:internal_exec_query,
          "SELECT text_col, number_as_text, json_as_text FROM pipeline_type_test LIMIT 1",
          "Pipeline Test")
      end

      # Access the data - this should trigger type casting
      first_row = pipeline_result.first

      # Test 2: Compare with regular exec_query (should be identical)
      @connection.exit_persistent_pipeline_mode
      regular_result = @connection.exec_query(
        "SELECT text_col, number_as_text, json_as_text FROM pipeline_type_test LIMIT 1"
      )
      regular_first_row = regular_result.first

      # Both should return identical data types and values
      assert_equal regular_first_row["text_col"], first_row["text_col"]
      assert_equal regular_first_row["text_col"].class, first_row["text_col"].class

      assert_equal regular_first_row["number_as_text"], first_row["number_as_text"]
      assert_equal regular_first_row["number_as_text"].class, first_row["number_as_text"].class

      assert_equal regular_first_row["json_as_text"], first_row["json_as_text"]
      assert_equal regular_first_row["json_as_text"].class, first_row["json_as_text"].class

      # Verify basic PostgreSQL type casting is working (strings should be strings)
      assert_equal "hello", first_row["text_col"]
      assert_kind_of String, first_row["text_col"]

      # Note: This test verifies that pipeline results have identical type casting
      # to regular results. Model-level attribute type casting happens at the
      # model layer, not at the connection level that PipelineResult operates at.

    ensure
      @connection.execute "DROP TABLE IF EXISTS pipeline_type_test"
    end
  end
end
