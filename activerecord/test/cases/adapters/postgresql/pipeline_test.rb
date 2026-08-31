# frozen_string_literal: true

require "cases/helper"
require "models/author"

module ActiveRecord
  class PostgresqlPipelineTest < ActiveRecord::PostgreSQLTestCase
    self.use_transactional_tests = false

    def setup
      super
      @connection = ActiveRecord::Base.lease_connection
      @connection.connect!
      @connection.exit_pipeline_mode if @connection.pipeline_active?
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

    def test_pipeline_exit_closes_connection_when_flush_leaves_unread_results
      subscriber_error = StandardError.new("subscriber failed")
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |event|
        raise subscriber_error if event.payload[:sql] == "SELECT 1 AS n"
      end
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n")
      second = build_intent("SELECT 2 AS n")
      first.execute!
      @connection.pipeline_sync
      second.execute!

      assert_same subscriber_error, assert_raises(StandardError) { @connection.exit_pipeline_mode }

      assert_not_predicate @connection, :connected?
      assert_predicate first, :raw_result_available?
      assert_raises(ActiveRecord::ConnectionFailed) { second.raw_result }
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end

    def test_ineligible_synchronous_queries_exit_pipeline_mode
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      result = @connection.exec_query("SELECT 2 AS n; SELECT 3 AS n")

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], cast_rows(pending)
      assert_equal [[3]], result.rows
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

    def test_active_treats_an_aborted_pipeline_probe_as_healthy
      @connection.enter_pipeline_mode
      failing = build_intent("SELECT * FROM nonexistent_pipeline_table")
      aborted = build_intent("SELECT 1 AS n")
      failing.execute!
      aborted.execute!

      assert_predicate @connection, :active?
      assert_not_predicate failing, :raw_result_available?
      assert_not_predicate aborted, :raw_result_available?

      @connection.exit_pipeline_mode

      assert_raises(ActiveRecord::StatementInvalid) { failing.raw_result }
      assert_equal :server_aborted, aborted.not_run_reason
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
      assert_equal [[1]], cast_rows(pending)
    end

    def test_active_probe_does_not_commit_unsynced_work
      @connection.execute("DROP TABLE IF EXISTS pipeline_probe_test")
      @connection.execute("CREATE TABLE pipeline_probe_test (value integer)")
      @connection.enter_pipeline_mode
      insert = build_intent("INSERT INTO pipeline_probe_test VALUES (1)")
      insert.execute!

      assert_predicate @connection, :active?
      assert_not_predicate insert, :raw_result_available?

      observer = ActiveRecord::Base.connection_pool.checkout
      begin
        assert_equal 0, observer.select_value("SELECT COUNT(*) FROM pipeline_probe_test")
      ensure
        ActiveRecord::Base.connection_pool.checkin(observer)
      end

      @connection.flush_pipeline

      assert_equal 1, @connection.select_value("SELECT COUNT(*) FROM pipeline_probe_test")
    ensure
      @connection.execute("DROP TABLE IF EXISTS pipeline_probe_test")
    end

    def test_canceling_pipeline_future_drains_query_and_remains_canceled
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT 1 AS n")
      intent.execute!
      future_result = intent.future_result

      assert_predicate future_result, :pending?
      assert_same future_result, future_result.cancel

      assert_not_predicate future_result, :pending?
      assert_predicate future_result, :canceled?
      assert_not_predicate @connection, :pipeline_pending?
      assert_predicate intent, :finalized?
      assert_raises(ActiveRecord::FutureResult::Canceled) { future_result.result }
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

    def test_disconnect_cleans_up_when_pipeline_delivery_raises
      subscriber_error = StandardError.new("subscriber failed")
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |event|
        raise subscriber_error if event.payload[:sql] == "SELECT 1 AS n"
      end
      @connection.enter_pipeline_mode
      pending = build_intent("SELECT 1 AS n")
      pending.execute!

      assert_same subscriber_error, assert_raises(StandardError) { @connection.disconnect! }

      assert_not_predicate @connection, :connected?
      assert_nil @connection.instance_variable_get(:@raw_connection)
      assert_equal [[1]], pending.cast_result.rows
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
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

    def test_schema_search_path_write_does_not_trust_pipeline_status
      search_path = @connection.schema_search_path
      @connection.instance_variable_set(:@schema_search_path, nil)
      @connection.enter_pipeline_mode
      pending = add_query("SELECT 1 AS n")

      @connection.schema_search_path = search_path

      assert_predicate @connection, :pipeline_active?
      assert_not_predicate @connection, :pipeline_pending?
      assert_equal search_path, @connection.select_value("SHOW search_path")
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

    def test_bind_casting_precedes_pipeline_routing
      mapped_timezone = @connection.instance_variable_get(:@mapped_default_timezone)
      other_timezone = ActiveRecord.default_timezone == :utc ? :local : :utc
      @connection.instance_variable_set(:@mapped_default_timezone, other_timezone)
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT $1::int", binds: [42])

      intent.execute!

      assert_predicate @connection, :pipeline_active?
      assert_equal [[42]], intent.cast_result.rows
    ensure
      @connection.instance_variable_set(:@mapped_default_timezone, mapped_timezone)
    end

    def test_active_pipeline_routes_query_intents
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT 1 AS n")

      intent.execute!

      assert_predicate @connection, :pipeline_pending?
      assert_not_predicate intent, :raw_result_available?
      assert_equal [[1]], intent.cast_result.rows
      assert_predicate @connection, :pipeline_active?
      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_queries_execute_immediately_without_pipeline_capability
      @connection.stub(:supports_pipeline?, false) do
        intent = build_intent("SELECT 1 AS n")
        intent.execute!

        assert_predicate intent, :raw_result_available?
        assert_equal [[1]], intent.cast_result.rows
        assert_not_predicate @connection, :pipeline_active?
      end
    end

    def test_queries_outside_pipeline_execute_immediately
      skip "requires the normal routing policy" if ENV["AR_POSTGRESQL_PIPELINE"] == "1"

      intent = build_intent("SELECT 1 AS n")

      intent.execute!

      assert_predicate intent, :raw_result_available?
      assert_equal [[1]], intent.cast_result.rows
      assert_not_predicate @connection, :pipeline_active?
    end

    def test_prepared_queries_drain_pipeline_mode
      @connection.enter_pipeline_mode
      pending = build_intent("SELECT 1 AS n")
      prepared = build_intent("SELECT 2 AS n", prepare: true)

      pending.execute!
      prepared.execute!

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], pending.cast_result.rows
      assert_equal [[2]], prepared.cast_result.rows
    end

    def test_preparable_arel_queries_drain_pipeline_mode
      @connection.enter_pipeline_mode
      pending = build_intent("SELECT 1 AS n")
      preparable = ConnectionAdapters::QueryIntent.new(
        adapter: @connection,
        arel: Author.where(id: 1).arel,
        name: "PIPELINE",
        prepare: nil,
        materialize_transactions: false
      )

      pending.execute!
      preparable.execute!

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], pending.cast_result.rows
      assert_predicate preparable, :raw_result_available?
    end

    def test_multi_statement_queries_drain_pipeline_mode
      @connection.enter_pipeline_mode
      pending = build_intent("SELECT 1 AS n")
      multi_statement = build_intent("SELECT 2 AS n; SELECT 3 AS n")

      pending.execute!
      multi_statement.execute!

      assert_not_predicate @connection, :pipeline_active?
      assert_equal [[1]], pending.cast_result.rows
      assert_predicate multi_statement, :raw_result_available?
    end

    def test_pipelined_intent_dirties_transaction_on_delivery
      @connection.transaction do
        @connection.enter_pipeline_mode
        intent = build_intent("SELECT 1 AS n", materialize_transactions: true)

        intent.execute!

        assert_predicate @connection.transaction_manager, :restorable?
        assert_equal [[1]], intent.cast_result.rows
        assert_not_predicate @connection.transaction_manager, :restorable?
        raise ActiveRecord::Rollback
      end
    end

    def test_notification_errors_do_not_orphan_later_results
      subscriber_error_class = Class.new(Exception)
      subscriber_error = subscriber_error_class.new("subscriber failed")
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |event|
        raise subscriber_error if event.payload[:sql] == "SELECT 1 AS n"
      end

      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n")
      second = build_intent("SELECT 2 AS n")
      first.execute!
      second.execute!

      assert_same subscriber_error, assert_raises(subscriber_error_class) { first.raw_result }
      assert_predicate second, :raw_result_available?
      assert_equal [[2]], second.cast_result.rows
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
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

    def test_pipeline_error_rolls_back_earlier_write_in_implicit_transaction
      @connection.execute("DROP TABLE IF EXISTS pipeline_batch_test")
      @connection.execute("CREATE TABLE pipeline_batch_test (value text)")
      @connection.enter_pipeline_mode
      inserted = build_intent("INSERT INTO pipeline_batch_test (value) VALUES ('first')")
      failed = build_intent("INSERT INTO nonexistent_pipeline_table (value) VALUES ('second')")
      inserted.execute!
      failed.execute!

      @connection.flush_pipeline

      assert_equal 1, inserted.affected_rows
      assert_raises(ActiveRecord::StatementInvalid) { failed.raw_result }
      @connection.exit_pipeline_mode
      assert_equal 0, @connection.select_value("SELECT COUNT(*) FROM pipeline_batch_test")
    ensure
      @connection.exit_pipeline_mode if @connection.pipeline_active?
      @connection.execute("DROP TABLE IF EXISTS pipeline_batch_test") if @connection.connected?
    end

    def test_not_run_query_does_not_recover_inside_failed_transaction
      @connection.transaction do
        @connection.enter_pipeline_mode
        failed = build_intent("SELECT * FROM pipeline_missing_table", materialize_transactions: true)
        not_run = build_intent("SELECT 1 AS n", materialize_transactions: true)
        failed.execute!
        not_run.execute!
        @connection.flush_pipeline

        assert_raises(ActiveRecord::StatementInvalid) { failed.raw_result }
        assert_equal :server_aborted, not_run.not_run_reason
        error = assert_raises(ActiveRecord::StatementInvalid) { not_run.raw_result }
        assert_match(/current transaction is aborted/, error.message)

        raise ActiveRecord::Rollback
      end
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

    def test_pipeline_sync_probe_failure_replays_without_retry_permission
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT 1 AS n")
      intent.execute!
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_consume = raw_connection.method(:consume_input)
      consume_calls = 0
      failing_consume = lambda do
        consume_calls += 1
        raise PG::ConnectionBad, "closed" if consume_calls == 1

        original_consume.call
      end

      raw_connection.stub(:consume_input, failing_consume) do
        assert_equal [[1]], intent.cast_result.rows
      end

      assert_operator consume_calls, :>, 1
      assert_not_equal initial_connection_id, connection_id_from_server(@connection)
    end

    def test_pipeline_sync_send_failure_does_not_replay_without_retry_permission
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT 1 AS n")
      intent.execute!
      raw_connection = @connection.instance_variable_get(:@raw_connection)

      raw_connection.stub(:pipeline_sync, -> { raise PG::ConnectionBad, "closed" }) do
        @connection.flush_pipeline
      end

      assert_instance_of ActiveRecord::ConnectionNotEstablished, intent.error
      assert_nil intent.not_run_reason
      assert_raises(ActiveRecord::ConnectionNotEstablished) { intent.raw_result }
    end

    def test_enqueue_connection_failure_replays_pending_unsynced_queries
      initial_connection_id = @connection.instance_variable_get(:@raw_connection).backend_pid
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n")
      second = build_intent("SELECT 2 AS n")
      third = build_intent("SELECT 3 AS n")
      first.execute!
      second.execute!

      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      target_sql = [first, second, third].map(&:processed_sql)
      send_attempts = 0
      failing_send = lambda do |*args|
        if target_sql.include?(args.first)
          send_attempts += 1
          raise PG::ConnectionBad, "closed" if send_attempts == 1
        end

        original_send.call(*args)
      end

      raw_connection.stub(:send_query_params, failing_send) do
        third.execute!
      end

      assert_equal 4, send_attempts
      assert_not_equal initial_connection_id, raw_connection.backend_pid
      assert_predicate @connection, :pipeline_active?
      assert_equal [[[1]], [[2]], [[3]]], [first, second, third].map { |intent| intent.cast_result.rows }
    end

    def test_failed_reconnect_during_enqueue_recovery_settles_pending_intents
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n")
      second = build_intent("SELECT 2 AS n")
      third = build_intent("SELECT 3 AS n")
      first.execute!
      second.execute!

      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      failing_send = lambda do |sql, *binds|
        raise PG::ConnectionBad, "closed" if sql == third.processed_sql

        original_send.call(sql, *binds)
      end
      reconnect_error = ActiveRecord::ConnectionFailed.new("reconnect failed")
      original_ensure = @connection.method(:ensure_connection_ready)
      failing_reconnect = lambda do |**kwargs|
        if @connection.instance_variable_get(:@needs_reconnect)
          raise reconnect_error
        end

        original_ensure.call(**kwargs)
      end

      raised = @connection.stub(:ensure_connection_ready, failing_reconnect) do
        raw_connection.stub(:send_query_params, failing_send) do
          assert_raises(ActiveRecord::ConnectionFailed) { third.execute! }
        end
      end

      assert_same reconnect_error, raised
      assert_predicate first, :raw_result_available?
      assert_predicate second, :raw_result_available?
      assert_equal :unsynced, first.not_run_reason
      assert_equal :unsynced, second.not_run_reason
      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_repeated_enqueue_failure_settles_partially_replayed_queries
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n")
      second = build_intent("SELECT 2 AS n")
      third = build_intent("SELECT 3 AS n")
      first.execute!
      second.execute!

      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      target_sql = [first, second, third].map(&:processed_sql)
      send_attempts = 0
      failing_send = lambda do |*args|
        if target_sql.include?(args.first)
          send_attempts += 1
          raise PG::ConnectionBad, "closed" if send_attempts == 1 || send_attempts == 3
        end

        original_send.call(*args)
      end

      @connection.stub(:connection_retries, 1) do
        raw_connection.stub(:send_query_params, failing_send) do
          assert_raises(ActiveRecord::ConnectionNotEstablished) { third.execute! }
        end
      end

      assert_equal 3, send_attempts
      assert_not_predicate @connection, :pipeline_pending?
      assert_equal :unsynced, first.not_run_reason
      assert_equal :unsynced, second.not_run_reason
    end

    def test_enqueue_recovery_preserves_queued_and_unsent_order
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1")
      second = build_intent("SELECT 2")
      third = build_intent("SELECT 3")
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      window = replay_window(not_run: [first, second, third])
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      target_sql = [first, second, third].map(&:processed_sql)
      send_attempts = 0
      failing_send = lambda do |*args|
        if target_sql.include?(args.first)
          send_attempts += 1
          raise error if send_attempts == 2
        end

        original_send.call(*args)
      end

      raised = raw_connection.stub(:send_query_params, failing_send) do
        assert_raises(ActiveRecord::ConnectionFailed) do
          @connection.send(:submit_pipeline_replay, window)
        end
      end
      assert_same error, raised

      replayable = @connection.send(
        :recover_pipeline_enqueue,
        error,
        window,
        allow_recovery: true
      )

      assert_equal [first, second, third], replayable.to_a
    end

    def test_pending_window_replays_after_connection_loss
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      first = build_intent(sleeping_query(1, initial_connection_id), allow_retry: true)
      second = build_intent("SELECT 2 AS n", allow_retry: true)
      first.execute!
      second.execute!

      kill_connection_from_server(initial_connection_id)

      assert_equal [[1]], first.cast_result.rows
      assert_equal [[2]], second.cast_result.rows
      assert_not_equal initial_connection_id, connection_id_from_server(@connection)
    end

    def test_synced_retryable_pipeline_reconnects_and_replays
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      intent = build_intent(
        sleeping_query(1, initial_connection_id),
        allow_retry: true
      )
      intent.execute!
      @connection.pipeline_sync

      kill_connection_from_server(initial_connection_id)

      assert_equal [[1]], intent.cast_result.rows
      assert_not_equal initial_connection_id, connection_id_from_server(@connection)
    end

    def test_synced_pipeline_respects_zero_connection_retries
      @connection.stub(:connection_retries, 0) do
        @connection.enter_pipeline_mode
        intent = build_intent("SELECT 1 AS n", allow_retry: true)
        intent.execute!
        @connection.pipeline_sync
        close_client_socket(@connection)

        assert_raises(ActiveRecord::ConnectionFailed) { intent.raw_result }
      end
    end

    def test_expired_retry_deadline_blocks_pipeline_replay
      intent = build_intent("SELECT 1", allow_retry: true)
      intent.retry_budget = ConnectionAdapters::RetryBudget.new(
        retries: 1,
        deadline: Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1,
        reconnectable: true
      )
      sync = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent.new
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(:@pending_intents, [intent, sync])

      replayable = @connection.send(
        :abandon_pipelined_intents,
        error,
        allow_recovery: true
      )

      assert_nil replayable
      assert_same error, intent.error
    end

    def test_pipeline_replay_consumes_budget_at_frontier_sync_group
      frontier = build_intent("SELECT 1", allow_retry: true)
      frontier_peer = build_intent("SELECT 2", allow_retry: true)
      later = build_intent("SELECT 3", allow_retry: true)
      [frontier, frontier_peer, later].each do |intent|
        intent.retry_budget = ConnectionAdapters::RetryBudget.new(
          retries: 2,
          deadline: nil,
          reconnectable: true
        )
      end
      window = replay_window(groups: [
        { uncertain: [frontier, frontier_peer], sync: true },
        { uncertain: [later], sync: true },
      ])
      error = ActiveRecord::ConnectionFailed.new("connection closed")

      assert @connection.send(:consume_pipeline_retry_budgets, error, window)

      assert_equal 1, frontier.retry_budget.retries_remaining
      assert_equal 1, frontier_peer.retry_budget.retries_remaining
      assert_equal 2, later.retry_budget.retries_remaining
      assert_not_predicate frontier.retry_budget, :reconnectable?
      assert_not_predicate frontier_peer.retry_budget, :reconnectable?
      assert_predicate later.retry_budget, :reconnectable?
    end

    def test_multiple_synced_queries_replay_together
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      intents = [
        build_intent(sleeping_query(1, initial_connection_id), allow_retry: true),
        build_intent("SELECT 2 AS n", allow_retry: true),
        build_intent("SELECT 3 AS n", allow_retry: true),
      ]
      intents.each(&:execute!)
      @connection.pipeline_sync

      kill_connection_from_server(initial_connection_id)

      assert_equal [[[1]], [[2]], [[3]]], intents.map { |intent| intent.cast_result.rows }
      assert_not_equal initial_connection_id, connection_id_from_server(@connection)
    end

    def test_replay_preserves_query_execution_order
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      create = build_intent(
        "CREATE TEMP TABLE pipeline_order_test ON COMMIT DROP AS " \
          "SELECT 42 AS value FROM #{sleep_on_connection(initial_connection_id)}",
        allow_retry: true
      )
      insert = build_intent(
        "INSERT INTO pipeline_order_test VALUES (43)",
        allow_retry: true
      )
      select = build_intent(
        "SELECT value FROM pipeline_order_test ORDER BY value",
        allow_retry: true
      )
      create.execute!
      insert.execute!
      select.execute!
      @connection.pipeline_sync

      kill_connection_from_server(initial_connection_id)

      assert_equal 1, create.affected_rows
      assert_equal 1, insert.affected_rows
      assert_equal [[42], [43]], select.cast_result.rows
    end

    def test_replay_preserves_sync_group_boundaries
      @connection.enter_pipeline_mode
      create = build_intent(
        "CREATE TEMP TABLE pipeline_replay_group_test (value integer)",
        allow_retry: true
      )
      create.execute!
      @connection.pipeline_sync
      failing = build_intent(
        "SELECT * FROM nonexistent_pipeline_replay_group_table",
        allow_retry: true
      )
      failing.execute!
      @connection.pipeline_sync

      close_client_socket(@connection)
      @connection.flush_pipeline

      assert_equal 0, create.affected_rows
      assert_raises(ActiveRecord::StatementInvalid) { failing.raw_result }
      assert_equal "pipeline_replay_group_test",
        @connection.select_value("SELECT to_regclass('pg_temp.pipeline_replay_group_test')::text")
    end

    def test_completed_sync_group_is_not_replayed
      @connection.execute("DROP TABLE IF EXISTS pipeline_replay_write_test")
      @connection.execute("CREATE TABLE pipeline_replay_write_test (value integer)")
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      insert = build_intent(
        "INSERT INTO pipeline_replay_write_test VALUES (1)",
        allow_retry: true
      )
      insert.execute!
      @connection.pipeline_sync
      assert_equal 1, insert.affected_rows

      pending = build_intent(
        sleeping_query(1, initial_connection_id),
        allow_retry: true
      )
      pending.execute!
      @connection.pipeline_sync
      kill_connection_from_server(initial_connection_id)

      assert_equal [[1]], pending.cast_result.rows
      assert_equal 1, @connection.select_value("SELECT COUNT(*) FROM pipeline_replay_write_test")
    ensure
      @connection.execute("DROP TABLE IF EXISTS pipeline_replay_write_test")
    end

    def test_synced_retryable_pipeline_replays_in_clean_transaction
      initial_connection_id = connection_id_from_server(@connection)

      @connection.transaction do
        @connection.enter_pipeline_mode
        first = build_intent("SELECT 1 AS n", allow_retry: true)
        second = build_intent("SELECT 2 AS n", allow_retry: true)
        first.execute!
        second.execute!
        @connection.pipeline_sync
        close_client_socket(@connection)

        assert_equal [[1]], first.cast_result.rows
        assert_equal [[2]], second.cast_result.rows
        assert_not_equal initial_connection_id, connection_id_from_server(@connection)
        raise ActiveRecord::Rollback
      end
    end

    def test_unsynced_pipeline_replays_in_clean_transaction
      initial_connection_id = connection_id_from_server(@connection)

      @connection.transaction do
        @connection.enter_pipeline_mode
        first = build_intent("SELECT 1 AS n")
        second = build_intent("SELECT 2 AS n")
        first.execute!
        second.execute!
        close_client_socket(@connection)

        assert_equal [[1]], first.cast_result.rows
        assert_equal [[2]], second.cast_result.rows
        assert_not_equal initial_connection_id, connection_id_from_server(@connection)
        raise ActiveRecord::Rollback
      end
    end

    def test_dirty_transaction_blocks_pipeline_replay
      initial_connection_id = connection_id_from_server(@connection)

      assert_raises(ActiveRecord::ConnectionFailed) do
        @connection.transaction do
          @connection.select_value("SELECT 0")
          @connection.enter_pipeline_mode
          intent = build_intent(
            sleeping_query(1, initial_connection_id),
            allow_retry: true,
            materialize_transactions: true
          )
          intent.execute!
          @connection.pipeline_sync

          kill_connection_from_server(initial_connection_id)
          intent.raw_result
        end
      end
    end

    def test_synced_nonretryable_pipeline_delivers_connection_failure
      @connection.enter_pipeline_mode
      intent = build_intent("SELECT 1 AS n")
      intent.execute!
      @connection.pipeline_sync
      close_client_socket(@connection)

      assert_raises(ActiveRecord::ConnectionFailed) { intent.raw_result }
    end

    def test_replay_preserves_pending_intent_order
      first = build_intent("SELECT 1", allow_retry: true)
      second = build_intent("SELECT 2", allow_retry: true)
      sync = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent.new
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(:@pending_intents, [first, second, sync])

      replayable = @connection.send(
        :abandon_pipelined_intents,
        error,
        allow_recovery: true
      )

      assert_equal [first, second], replayable.to_a
      assert_not_predicate first, :raw_result_available?
      assert_not_predicate second, :raw_result_available?
    end

    def test_replay_stops_when_the_leading_intent_makes_no_progress
      first = build_intent("SELECT 1", allow_retry: true)
      second = build_intent("SELECT 2", allow_retry: true)
      sync = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent.new
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(:@pending_intents, [first, second, sync])

      replayable = @connection.send(
        :abandon_pipelined_intents,
        error,
        allow_recovery: true,
        last_replayed_head: first
      )

      assert_nil replayable
      assert_same error, first.error
      assert_same error, second.error
    end

    def test_replay_no_progress_makes_replay_safe_intents_terminal
      first = build_intent("SELECT 1", allow_retry: true)
      second = build_intent("SELECT 2", allow_retry: true)
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(:@pending_intents, [first, second])

      replayable = @connection.send(
        :abandon_pipelined_intents,
        error,
        allow_recovery: true,
        last_replayed_head: first
      )

      assert_nil replayable
      assert_same error, first.error
      assert_same error, second.error
      assert_predicate first, :raw_result_available?
      assert_predicate second, :raw_result_available?
    end

    def test_failed_reconnect_settles_replay_intents
      uncertain = build_intent("SELECT 1", allow_retry: true)
      not_run = build_intent("SELECT 2", allow_retry: true)
      error = ActiveRecord::ConnectionFailed.new("reconnect failed")
      window = replay_window(uncertain: [uncertain], not_run: [not_run])

      replayed = @connection.stub(:reconnect!, ->(**) { raise error }) do
        @connection.send(:replay_pipeline_intents, window)
      end

      assert_not replayed
      assert_same error, uncertain.error
      assert_equal :unsynced, not_run.not_run_reason
      assert_predicate uncertain, :raw_result_available?
      assert_predicate not_run, :raw_result_available?
    end

    def test_replay_send_failure_settles_the_unsent_tail
      first = build_intent("SELECT 1", allow_retry: true)
      second = build_intent("SELECT 2", allow_retry: true)
      third = build_intent("SELECT 3", allow_retry: true)
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      target_sql = [first, second, third].map(&:processed_sql)
      send_attempts = 0
      failing_send = lambda do |*args|
        if target_sql.include?(args.first)
          send_attempts += 1
          raise PG::ConnectionBad, "closed" if send_attempts == 2
        end

        original_send.call(*args)
      end

      window = replay_window(uncertain: [first], not_run: [second, third])
      replayed = @connection.stub(:connection_retries, 1) do
        raw_connection.stub(:send_query_params, failing_send) do
          @connection.send(:replay_pipeline_intents, window)
        end
      end

      assert_not replayed
      assert_equal 2, send_attempts
      assert_not_predicate @connection, :pipeline_pending?
      assert_instance_of ActiveRecord::ConnectionNotEstablished, first.error
      assert_nil first.not_run_reason
      assert_equal :unsynced, second.not_run_reason
      assert_equal :unsynced, third.not_run_reason
    end

    def test_replay_send_failure_preserves_completed_group_boundary
      first = build_intent("SELECT 1", allow_retry: true)
      second = build_intent("SELECT 2", allow_retry: true)
      window = replay_window(groups: [
        { uncertain: [first], sync: true },
        { not_run: [second], sync: false },
      ])
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      target_sql = [first, second].map(&:processed_sql)
      send_attempts = 0
      failing_send = lambda do |*args|
        if target_sql.include?(args.first)
          send_attempts += 1
          raise PG::ConnectionBad, "closed" if send_attempts == 2
        end

        original_send.call(*args)
      end

      replayed = raw_connection.stub(:send_query_params, failing_send) do
        @connection.send(:replay_pipeline_intents, window)
      end

      assert_not replayed
      assert_equal 2, send_attempts
      assert_instance_of ActiveRecord::ConnectionNotEstablished, first.error
      assert_equal :unsynced, second.not_run_reason
      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_unexpected_replay_send_failure_settles_the_unsent_tail
      first = build_intent("SELECT 1", allow_retry: true)
      second = build_intent("SELECT 2", allow_retry: true)
      raw_connection = @connection.instance_variable_get(:@raw_connection)
      original_send = raw_connection.method(:send_query_params)
      target_sql = [first, second].map(&:processed_sql)
      send_attempts = 0
      failing_send = lambda do |*args|
        if target_sql.include?(args.first)
          send_attempts += 1
          raise "unexpected send failure" if send_attempts == 2
        end

        original_send.call(*args)
      end
      window = replay_window(not_run: [first, second])

      error = raw_connection.stub(:send_query_params, failing_send) do
        assert_raises(RuntimeError) { @connection.send(:replay_pipeline_intents, window) }
      end

      assert_equal "unexpected send failure", error.message
      assert_not_predicate @connection, :pipeline_pending?
      assert_equal :unsynced, first.not_run_reason
      assert_equal :unsynced, second.not_run_reason
    end

    def test_connection_failure_distinguishes_synced_and_unsynced_intents
      synced = build_intent("SELECT 1")
      unsynced = build_intent("SELECT 2")
      sync = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent.new
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(:@pending_intents, [synced, sync, unsynced])

      @connection.send(:abandon_pipelined_intents, error)

      assert_same error, synced.error
      assert_nil synced.not_run_reason
      assert_nil unsynced.error
      assert_equal :unsynced, unsynced.not_run_reason
    end

    def test_connection_failure_uses_the_last_sync_boundary
      first = build_intent("SELECT 1")
      second = build_intent("SELECT 2")
      third = build_intent("SELECT 3")
      sync_class = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(:@pending_intents, [first, sync_class.new, second, sync_class.new, third])

      @connection.send(:abandon_pipelined_intents, error)

      assert_same error, first.error
      assert_same error, second.error
      assert_equal :unsynced, third.not_run_reason
    end

    def test_server_fatal_result_requires_retry_through_the_failing_intent
      synced = build_intent("SELECT 0")
      first = build_intent("SELECT 1")
      fatal = build_intent("SELECT 2")
      third = build_intent("SELECT 3")
      sync_class = ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent
      error = ActiveRecord::ConnectionFailed.new("connection closed")
      @connection.instance_variable_set(
        :@pending_intents,
        [synced, sync_class.new, first, fatal, third, sync_class.new]
      )

      @connection.send(:abandon_pipelined_intents, error, server_fatal_intent: fatal)

      assert_same error, synced.error
      assert_same error, first.error
      assert_same error, fatal.error
      assert_equal :unsynced, third.not_run_reason
    end

    def test_server_fatal_result_uses_the_failing_intents_retry_permission
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n", allow_retry: true)
      fatal = build_intent(terminate_original_connection(initial_connection_id))
      third = build_intent("SELECT 3 AS n")
      first.execute!
      fatal.execute!
      third.execute!

      @connection.flush_pipeline

      assert_instance_of ActiveRecord::ConnectionFailed, first.error
      assert_instance_of ActiveRecord::ConnectionFailed, fatal.error
      assert_equal :unsynced, third.not_run_reason
    end

    def test_server_fatal_result_replays_safe_suffix_without_retry_permission
      initial_connection_id = connection_id_from_server(@connection)
      @connection.enter_pipeline_mode
      first = build_intent("SELECT 1 AS n", allow_retry: true)
      fatal = build_intent(terminate_original_connection(initial_connection_id), allow_retry: true)
      third = build_intent("SELECT 3 AS n")
      first.execute!
      fatal.execute!
      third.execute!

      @connection.flush_pipeline

      assert_equal [[1]], first.cast_result.rows
      assert_equal [[true, nil]], fatal.cast_result.rows
      assert_equal [[3]], third.cast_result.rows
      assert_not_equal initial_connection_id, connection_id_from_server(@connection)
    end

    def test_server_fatal_replay_stops_when_no_progress
      @connection.enter_pipeline_mode
      fatal = build_intent(
        "SELECT pg_terminate_backend(pg_backend_pid()), pg_sleep(5)",
        allow_retry: true
      )
      fatal.execute!

      @connection.flush_pipeline

      assert_raises(ActiveRecord::ConnectionFailed) { fatal.raw_result }
      assert_predicate fatal, :raw_result_available?
    end

    def test_connection_error_translation_prefers_captured_fatal_notice
      notice_error = PG::AdminShutdown.new("terminating connection")
      @connection.instance_variable_set(:@notice_receiver_fatal_error, notice_error)

      translated = @connection.send(
        :translate_pipeline_connection_error,
        PG::Error.new("server closed the connection")
      )

      assert_same notice_error, translated.cause
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
      def capture_sql_payloads(&block)
        events = []
        callback = ->(_name, _start, _finish, _id, payload) { events << payload.dup }

        ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)

        events
      end

      def find_sql_payload(events, sql)
        events.find { |payload| payload[:sql] == sql } ||
          flunk("Expected sql.active_record event for #{sql.inspect}")
      end

      def sleeping_query(value, connection_id)
        "SELECT #{value} AS n FROM #{sleep_on_connection(connection_id)}"
      end

      def sleep_on_connection(connection_id)
        "pg_sleep(CASE WHEN pg_backend_pid() = #{connection_id} THEN 2 ELSE 0 END)"
      end

      def terminate_original_connection(connection_id)
        "SELECT" \
          " CASE WHEN pg_backend_pid() = #{connection_id}" \
          " THEN pg_terminate_backend(pg_backend_pid()) ELSE true END," \
          " CASE WHEN pg_backend_pid() = #{connection_id}" \
          " THEN pg_sleep(5)::text END"
      end

      def replay_window(uncertain: [], not_run: [], groups: nil)
        replay_window_class = ConnectionAdapters::PostgreSQL::PipelineContext.const_get(:ReplayWindow, false)
        groups ||= [{ uncertain: uncertain, not_run: not_run, sync: false }]
        retry_required_through = nil
        operations = groups.flat_map do |group|
          uncertain = group.fetch(:uncertain, [])
          retry_required_through = uncertain.last if uncertain.any?
          operations = uncertain + group.fetch(:not_run, [])
          operations << ConnectionAdapters::PostgreSQL::PipelineContext::SyncIntent.new if group.fetch(:sync)
          operations
        end
        if retry_required_through
          retry_required_index = operations.index(retry_required_through)
          replay_window_class.new(
            retry_required: operations[0..retry_required_index],
            replay_safe: operations[(retry_required_index + 1)..] || []
          )
        else
          replay_window_class.new(replay_safe: operations)
        end
      end

      def build_intent(sql, binds: [], prepare: false, allow_retry: false, materialize_transactions: false)
        ConnectionAdapters::QueryIntent.new(
          adapter: @connection,
          raw_sql: sql,
          name: "PIPELINE",
          binds: binds,
          prepare: prepare,
          allow_retry: allow_retry,
          materialize_transactions: materialize_transactions
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

  class PostgresqlPipelineFixtureConcurrencyTest < ActiveRecord::PostgreSQLTestCase
    def setup
      super
      @pool = ActiveRecord::Base.connection_pool
      @connection = ActiveRecord::Base.lease_connection
    end

    def teardown
      @connection.exit_pipeline_mode if @connection.pipeline_active?
      super
    end

    def test_concurrent_enqueue_observation_and_flush_on_pinned_connection
      connections = Queue.new
      promises = 4.times.map do |value|
        Thread.new do
          @pool.with_connection do |connection|
            connections << connection
            connection.select_value("SELECT #{value}", "TEST", pipeline: true)
          end
        end
      end.map(&:value)

      4.times { assert_same @connection, connections.pop }
      assert promises.all?(&:pending?)

      ready = Queue.new
      readers = promises.map do |promise|
        Thread.new do
          ready.pop
          promise.value
        end
      end
      flushers = 4.times.map do
        Thread.new do
          ready.pop
          @pool.with_connection(&:flush_pipeline)
        end
      end
      (readers.length + flushers.length).times { ready << true }

      flushers.each(&:value)
      assert_equal [0, 1, 2, 3], readers.map(&:value).sort
      assert_not_predicate @connection, :pipeline_pending?
    end

    def test_result_observation_can_race_synchronous_barrier
      promise = @connection.select_value("SELECT 1", "TEST", pipeline: true)
      ready = Queue.new

      reader = Thread.new do
        ready.pop
        promise.value
      end
      barrier = Thread.new do
        ready.pop
        @pool.with_connection do |connection|
          connection.select_value("SELECT 2; SELECT 3")
        end
      end
      2.times { ready << true }

      assert_equal 1, reader.value
      assert_equal 3, barrier.value
      assert_not_predicate @connection, :pipeline_active?
      assert_not_predicate @connection, :pipeline_pending?
    end
  end

  class PostgresqlPipelineLeaseTest < ActiveRecord::PostgreSQLTestCase
    self.use_transactional_tests = false

    def setup
      super
      pool_config = ActiveRecord::Base.connection_pool.db_config
      @pool = ConnectionAdapters::ConnectionPool.new(
        ConnectionAdapters::PoolConfig.new(ActiveRecord::Base, pool_config, :writing, :default)
      )
    end

    def teardown
      @pool.disconnect!
      super
    end

    def test_pending_pipeline_holds_auto_managed_lease
      intent = nil
      connection = nil

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        intent = build_intent(conn)
        intent.execute!
      end

      assert_same connection, @pool.active_connection?
      assert_predicate connection, :pipeline_pending?
      assert_not_predicate intent, :raw_result_available?

      original_cast = connection.method(:cast_result)
      rows = connection.stub(:cast_result, ->(result) do
        assert_same connection, @pool.active_connection?
        original_cast.call(result)
      end) { intent.cast_result.rows }

      assert_equal [[1]], rows
      assert_not_predicate @pool, :active_connection?
    end

    def test_pipeline_drained_inside_later_block_releases_on_block_exit
      intent = nil
      connection = nil

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        intent = build_intent(conn)
        intent.execute!
      end

      @pool.with_connection do |conn|
        assert_same connection, conn
        assert_equal [[1]], intent.cast_result.rows
        assert_same connection, @pool.active_connection?
      end

      assert_not_predicate @pool, :active_connection?
    end

    def test_nested_connection_blocks_retain_lease_until_outer_exit
      intent = nil
      connection = nil

      @pool.with_connection do |outer_connection|
        connection = outer_connection
        connection.connect!
        connection.enter_pipeline_mode
        intent = build_intent(connection)
        intent.execute!

        @pool.with_connection do |inner_connection|
          assert_same connection, inner_connection
          assert_equal [[1]], intent.cast_result.rows
        end

        assert_predicate connection, :in_use?
      end

      assert_not_predicate connection, :in_use?
    end

    def test_sticky_lease_remains_after_pipeline_drains
      intent = nil
      connection = nil

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        intent = build_intent(conn)
        intent.execute!
      end

      assert_same connection, @pool.lease_connection
      assert_equal [[1]], intent.cast_result.rows
      assert_same connection, @pool.active_connection?
    ensure
      @pool.release_connection
    end

    def test_resettable_not_run_outcome_keeps_lease_until_rerun
      failing = nil
      not_run = nil
      connection = nil

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        failing = build_intent(conn, "SELECT * FROM pipeline_missing_table")
        not_run = build_intent(conn, "SELECT 1 AS n")
        failing.execute!
        not_run.execute!
      end

      assert_same connection, @pool.active_connection?
      assert_equal [[1]], not_run.cast_result.rows
      assert_not_predicate @pool, :active_connection?
      assert_predicate failing, :finalized?
      assert_predicate not_run, :finalized?
    end

    def test_explicit_release_terminalizes_not_run_outcome
      connection = nil
      not_run = nil

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        failing = build_intent(conn, "SELECT * FROM pipeline_missing_table")
        not_run = build_intent(conn, "SELECT 1 AS n")
        failing.execute!
        not_run.execute!
      end

      @pool.release_connection

      assert_not_predicate connection, :pipeline_active?
      error = assert_raises(ActiveRecord::QueryNotRun) { not_run.cast_result }
      assert_equal "Query was not run due to pipeline failure (server_aborted)", error.message
      assert_equal :server_aborted, error.reason
    end

    def test_release_discards_connection_when_pipeline_delivery_raises
      connection = nil
      intent = nil
      subscriber_error = StandardError.new("subscriber failed")
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |event|
        raise subscriber_error if event.payload[:sql] == "SELECT 1 AS n"
      end

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        intent = build_intent(conn)
        intent.execute!
      end

      assert_same subscriber_error, assert_raises(StandardError) { @pool.release_connection }

      assert_not_predicate @pool, :active_connection?
      assert_not_includes @pool.connections, connection
      assert_not_predicate connection, :connected?
      assert_not_predicate connection, :pipeline_active?
      assert_equal [[1]], intent.cast_result.rows
      @pool.with_connection { |conn| assert_not_same connection, conn }
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end

    def test_explicit_release_drains_pending_pipeline
      intent = nil
      connection = nil

      @pool.with_connection do |conn|
        connection = conn
        conn.connect!
        conn.enter_pipeline_mode
        intent = build_intent(conn)
        intent.execute!
      end

      @pool.release_connection

      assert_not_predicate @pool, :active_connection?
      assert_not_predicate connection, :pipeline_active?
      assert_equal [[1]], intent.cast_result.rows
    end

    private
      def build_intent(connection, sql = "SELECT 1 AS n")
        ConnectionAdapters::QueryIntent.new(
          adapter: connection,
          raw_sql: sql,
          name: "PIPELINE",
          materialize_transactions: false
        )
      end
  end
end
