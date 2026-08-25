# frozen_string_literal: true

require "cases/helper"

module ActiveRecord
  class PostgresqlPipelineTest < ActiveRecord::PostgreSQLTestCase
    self.use_transactional_tests = false

    def setup
      super
      @connection = ActiveRecord::Base.lease_connection
      @connection.connect!
      @original_db_warnings_action = :ignore
    end

    def teardown
      @connection.exit_pipeline_mode if @connection.pipeline_active?
      ActiveRecord::Base.connection_pool.release_connection
      super
    end

    def test_pipeline_mode_lifecycle
      assert_not_predicate @connection, :pipeline_active?

      @connection.enter_pipeline_mode
      assert_predicate @connection, :pipeline_active?

      @connection.exit_pipeline_mode
      assert_not_predicate @connection, :pipeline_active?
    end

    def test_pipeline_capability_can_be_unavailable
      @connection.stub(:supports_pipeline?, false) do
        assert_not_predicate @connection, :pipeline_active?
        assert_raises(NotImplementedError) { @connection.enter_pipeline_mode }
      end
    end

    def test_exit_pipeline_mode_drains_pending_queries
      @connection.enter_pipeline_mode
      intent = add_query("SELECT 1 AS n")

      @connection.exit_pipeline_mode

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], cast_rows(intent)
    end

    def test_clean_pipeline_exit_does_not_send_an_extra_sync
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_sync = raw_connection.method(:pipeline_sync)
      sync_calls = 0

      raw_connection.stub(:pipeline_sync, -> { sync_calls += 1; original_sync.call }) do
        @connection.enter_pipeline_mode
        intent = add_query("SELECT 1 AS n")

        @connection.exit_pipeline_mode

        assert_equal [[1]], cast_rows(intent)
      end

      assert_equal 1, sync_calls
      assert_not_predicate @connection, :pipeline_active?
    end

    def test_synchronous_queries_exit_pipeline_mode
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      result = @connection.exec_query("SELECT 2 AS n")

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], cast_rows(pending)
      assert_equal [[2]], result.rows
    end

    def test_raw_connection_access_exits_pipeline_mode
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      raw_connection = @connection.raw_connection

      assert_instance_of PG::Connection, raw_connection
      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], cast_rows(pending)
    end

    def test_raw_connection_yields_lock_pipeline_mode
      @connection.enter_pipeline_mode

      @connection.send(:with_raw_connection, materialize_transactions: false) do
        error = assert_raises(RuntimeError) do
          @connection.exit_pipeline_mode
        end
        assert_match(/pipelining is locked/, error.message)
      end

      assert_predicate @connection, :pipeline_active?
    end

    def test_active_probes_unsynced_pipeline_without_syncing
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      assert_predicate @connection, :active?

      assert_predicate @connection, :pipeline_active?
      assert_not_predicate pending, :raw_result_available?
      assert_equal [pending], @connection.instance_variable_get(:@pending_intents)
      assert_equal 1, @connection.instance_variable_get(:@pipeline_buffer).length

      @connection.exit_pipeline_mode

      assert_equal [[1]], cast_rows(pending)
    end

    def test_active_drains_pipeline_that_already_ends_in_sync
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")
      @connection.pipeline_sync

      assert_predicate @connection, :active?

      assert_predicate @connection, :pipeline_active?
      assert_predicate pending, :raw_result_available?
      assert_not_predicate @connection, :pipeline_pending?
      assert_equal [[1]], cast_rows(pending)
    end

    def test_failed_active_probe_keeps_unsynced_work_pending
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")
      raw_connection = @connection.instance_variable_get(:@raw_connection)

      raw_connection.stub(:send_query_params, ->(*) { raise PG::ConnectionBad, "closed" }) do
        assert_not_predicate @connection, :active?
      end

      assert_equal [pending], @connection.instance_variable_get(:@pending_intents)
      assert_not_predicate pending, :raw_result_available?
    end

    def test_checkin_drains_and_exits_pipeline_mode
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")
      connection = @connection

      ActiveRecord::Base.connection_pool.release_connection

      assert_not_predicate connection, :pipeline_active?
      assert_equal [[1]], cast_rows(pending)
    ensure
      @connection = ActiveRecord::Base.lease_connection
      @connection.connect!
    end

    def test_disconnect_drains_pipeline_mode
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      @connection.disconnect!

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], cast_rows(pending)
    end

    def test_prepare_statement_rejects_pipeline_mode
      @connection.enter_pipeline_mode
      raw_connection = @connection.instance_variable_get(:@raw_connection)

      error = assert_raises(RuntimeError) do
        @connection.send(:prepare_statement, "SELECT 1", [], raw_connection)
      end
      assert_match(/pipeline mode does not support prepared statements/, error.message)
    end

    def test_statement_pool_eviction_exits_pipeline_mode
      pool_config = ActiveRecord::Base.connection_pool.db_config
      test_config = pool_config.configuration_hash.merge(statement_limit: 2)
      test_pool = ConnectionAdapters::ConnectionPool.new(
        ConnectionAdapters::PoolConfig.new(
          ActiveRecord::Base,
          DatabaseConfigurations::HashConfig.new("test", "primary", test_config),
          :writing,
          :default
        )
      )

      connection = test_pool.checkout
      connection.exec_query("SELECT 1", "SQL", [], prepare: true)
      connection.exec_query("SELECT 2", "SQL", [], prepare: true)
      connection.enter_pipeline_mode

      statements = connection.instance_variable_get(:@statements)
      evicted_key = statements.first[1]
      statements["new_statement"] = "a999"

      assert_not_predicate connection, :pipeline_active?
      prepared = connection.select_values("SELECT name FROM pg_prepared_statements")
      assert_not_includes prepared, evicted_key
    ensure
      test_pool&.disconnect!
    end

    def test_schema_search_path_write_exits_pipeline_mode
      search_path = @connection.schema_search_path
      @connection.instance_variable_set(:@schema_search_path, nil)
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      @connection.schema_search_path = search_path

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], cast_rows(pending)
    end

    def test_queries_require_pipeline_mode
      intent = build_intent("SELECT 1")

      assert_raises(RuntimeError, "Pipeline mode not active") do
        @connection.pipeline_add_query(intent)
      end
    end

    def test_queries_are_recorded_only_after_they_are_sent
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT 1")
      raw_connection = @connection.instance_variable_get(:@raw_connection)

      raw_connection.stub(:send_query_params, ->(*) { raise PG::ConnectionBad, "closed" }) do
        assert_raises(PG::ConnectionBad) do
          @connection.pipeline_add_query(intent)
        end
      end

      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_pipeline_delivers_batched_query_results
      @connection.enter_pipeline_mode
      first = add_query("SELECT 1 AS n")
      second = add_query("SELECT 2 AS n")

      assert_predicate @connection, :pipeline_pending?
      assert_not_predicate first, :raw_result_available?
      assert_not_predicate second, :raw_result_available?

      @connection.flush_pipeline

      assert_not_predicate @connection, :pipeline_pending?
      assert_equal [[1]], cast_rows(first)
      assert_equal [[2]], cast_rows(second)
    end

    def test_pipeline_delivers_multiple_sync_groups
      @connection.enter_pipeline_mode
      first = add_query("SELECT 1 AS n")
      @connection.pipeline_sync
      second = add_query("SELECT 2 AS n")

      @connection.flush_pipeline

      assert_equal [[1]], cast_rows(first)
      assert_equal [[2]], cast_rows(second)
      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_pipelined_query_error_preserves_native_diagnostics
      @connection.enter_pipeline_mode
      failed = add_query("SELECT * FROM pipeline_missing_table")
      @connection.flush_pipeline

      error = assert_raises(ActiveRecord::StatementInvalid) { failed.raw_result }

      assert_kind_of PG::UndefinedTable, error.cause
      assert_equal "42P01", error.cause.result.error_field(PG::PG_DIAG_SQLSTATE)
    end

    def test_query_errors_are_delivered_after_pipeline_sync
      @connection.enter_pipeline_mode
      failed = add_query("SELECT * FROM pipeline_missing_table")
      not_run = add_query("SELECT 1 AS n")

      assert_nothing_raised do
        @connection.flush_pipeline
      end

      error = assert_raises(ActiveRecord::StatementInvalid) { failed.raw_result }
      assert_match(/pipeline_missing_table/, error.message)
      assert_equal :server_aborted, not_run.not_run_reason
      assert_equal [[1]], cast_rows(not_run)
      assert_nil not_run.not_run_reason
    end

    def test_warnings_are_delivered_with_their_query
      warnings = []
      warning_action = ->(warning) { warnings << warning.message }

      with_db_warnings_action(warning_action) do
        @connection.connect!
        @connection.enter_pipeline_mode
        ordinary = add_query("SELECT 1")
        warned = add_query("DO $$ BEGIN RAISE WARNING 'pipeline warning'; END $$")

        @connection.flush_pipeline

        ordinary.raw_result
        assert_empty warnings
        warned.raw_result
        assert_equal ["pipeline warning"], warnings
        warned.raw_result
        assert_equal ["pipeline warning"], warnings
      end
    end

    def test_results_remain_pending_until_pipeline_sync
      @connection.enter_pipeline_mode
      intent = add_query("SELECT 1 AS n")
      flush_server_results_without_sync

      @connection.consume_pipeline
      assert_not_predicate intent, :raw_result_available?
      assert_predicate @connection, :pipeline_pending?
      assert_equal 1, @connection.instance_variable_get(:@pipeline_buffer).length

      @connection.pipeline_sync
      @connection.drain_pipeline

      assert_equal [[1]], cast_rows(intent)
      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_tentative_result_is_discarded_when_connection_closes_before_sync
      @connection.enter_pipeline_mode
      intent = add_query("SELECT 1 AS n")
      flush_server_results_without_sync
      raw_connection = @connection.instance_variable_get(:@raw_connection)

      raw_connection.stub(:pipeline_sync, true) do
        @connection.pipeline_sync
      end
      @connection.consume_pipeline
      assert_equal 1, @connection.instance_variable_get(:@pipeline_buffer).length

      close_client_socket(@connection)

      assert_raises(PG::Error, IOError, SystemCallError) do
        @connection.consume_pipeline
      end
      assert_not_predicate intent, :raw_result_available?
      assert_empty @connection.instance_variable_get(:@pipeline_buffer)
    end

    def test_consumption_does_not_read_results_while_connection_is_busy
      @connection.enter_pipeline_mode
      intent = add_query("SELECT pg_sleep(0.1)")
      raw_connection = @connection.instance_variable_get(:@raw_connection)

      raw_connection.stub(:is_busy, true) do
        raw_connection.stub(:get_result, -> { raise "get_result would block" }) do
          @connection.consume_pipeline
        end
      end

      assert_not_predicate intent, :raw_result_available?
      assert_predicate @connection, :pipeline_pending?
    end

    def test_query_results_are_collapsed_until_the_command_boundary
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      first_result = raw_connection.exec("SELECT 1 AS n")
      second_result = raw_connection.exec("SELECT 2 AS n")
      intent = build_intent("SELECT 2 AS n")
      sync = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent.new

      @connection.enter_pipeline_mode
      @connection.instance_variable_set(:@pending_intents, [intent, sync])

      results = [first_result, second_result, nil]
      raw_connection.stub(:consume_input, -> { }) do
        raw_connection.stub(:is_busy, false) do
          raw_connection.stub(:get_result, -> { results.shift }) do
            @connection.consume_pipeline
          end
        end
      end

      assert_predicate first_result, :cleared?
      assert_not_predicate second_result, :cleared?
      assert_not_predicate intent, :raw_result_available?
      assert_same second_result, @connection.instance_variable_get(:@pipeline_buffer).first[2]
    ensure
      @connection&.send(:discard_pipeline_buffer)
      @connection&.instance_variable_set(:@pending_intents, [])
    end

    def test_query_result_is_retained_until_the_command_boundary
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      result = raw_connection.exec("SELECT 1 AS n")
      intent = build_intent("SELECT 1 AS n")

      @connection.enter_pipeline_mode
      @connection.instance_variable_set(:@pending_intents, [intent])

      busy = [false, true]
      raw_connection.stub(:consume_input, -> { }) do
        raw_connection.stub(:is_busy, -> { busy.shift }) do
          raw_connection.stub(:get_result, -> { result }) do
            @connection.consume_pipeline
          end
        end
      end

      assert_not_predicate result, :cleared?
      assert_same result, @connection.instance_variable_get(:@pipeline_result_buffer)
      assert_empty @connection.instance_variable_get(:@pipeline_buffer)

      raw_connection.stub(:consume_input, -> { }) do
        raw_connection.stub(:is_busy, false) do
          raw_connection.stub(:get_result, -> { }) do
            @connection.consume_pipeline
          end
        end
      end

      assert_nil @connection.instance_variable_get(:@pipeline_result_buffer)
      assert_same result, @connection.instance_variable_get(:@pipeline_buffer).first[2]
    ensure
      @connection&.send(:discard_pipeline_buffer)
      @connection&.instance_variable_set(:@pending_intents, [])
    end

    def test_connection_errors_are_delivered_to_pending_intents
      @connection.enter_pipeline_mode
      first = add_query("SELECT 1 AS n")
      second = add_query("SELECT 2 AS n")
      connection_error = PG::ConnectionBad.new("connection closed")

      @connection.stub(:get_available_pipeline_query_result, -> { raise connection_error }) do
        @connection.flush_pipeline
      end

      assert_not_predicate @connection, :pipeline_pending?
      assert_raises(ActiveRecord::ConnectionNotEstablished) { first.raw_result }
      assert_raises(ActiveRecord::ConnectionNotEstablished) { second.raw_result }
    end

    private
      def build_intent(sql)
        ConnectionAdapters::QueryIntent.new(
          adapter: @connection,
          raw_sql: sql,
          name: "PIPELINE",
          materialize_transactions: false
        )
      end

      def add_query(sql)
        build_intent(sql).tap do |intent|
          @connection.pipeline_add_query(intent)
        end
      end

      def cast_rows(intent)
        @connection.send(:cast_result, intent.raw_result).rows
      end

      def close_client_socket(connection)
        connection.instance_variable_get(:@raw_connection).socket_io.shutdown(Socket::SHUT_RDWR)
      end

      def flush_server_results_without_sync
        raw_connection = @connection.instance_variable_get(:@raw_connection)
        raw_connection.send_flush_request
        raw_connection.flush

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
        while raw_connection.is_busy
          raw_connection.consume_input
          raise "timed out waiting for flushed pipeline result" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

          sleep 0.01
        end
      end
  end
end
