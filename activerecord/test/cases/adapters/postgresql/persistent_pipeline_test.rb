# frozen_string_literal: true

require "cases/helper"
require "support/connection_helper"

module ActiveRecord
  class PostgresqlPersistentPipelineTest < ActiveRecord::PostgreSQLTestCase
    include ConnectionHelper

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
      
      # Execute a query in pipeline mode (should create pending result)
      result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        result = @connection.exec_query("SELECT 1 as test_value")
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
      
      # Execute query in pipeline mode but don't access result data yet
      result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        result = @connection.exec_query("SELECT 42 as answer")
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

    def test_with_raw_connection_auto_sync_for_user_code
      # Enter pipeline mode and create pending results
      @connection.enter_persistent_pipeline_mode
      pipeline_result = nil
      @connection.send(:with_raw_connection, pipeline_mode: true) do |raw_conn|
        pipeline_result = @connection.exec_query("SELECT 123 as sync_test")
        assert_kind_of ActiveRecord::PipelineResult, pipeline_result
        assert pipeline_result.pending?
      end
      
      # Should have pending results before user code accesses connection
      assert @connection.has_pending_pipeline_results?
      
      # User code should trigger auto-sync
      @connection.send(:with_raw_connection) do |raw_conn|
        result = @connection.exec_query("SELECT 456 as user_query")
        assert_equal 456, result.first["user_query"]
      end
      
      # Pending results should be cleared by auto-sync
      assert_not @connection.has_pending_pipeline_results?
      # But pipeline mode should remain active
      assert @connection.pipeline_mode?
      
      # Now we can access the pipeline result data
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
        assert_equal 'compatibility', result.first["test"]
      end
      
      # Should exit pipeline mode after block
      assert_not @connection.pipeline_mode?
    end
  end
end