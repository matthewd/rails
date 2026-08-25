# frozen_string_literal: true

require "cases/helper"
require "models/post"

class PipelinedQueriesTest < ActiveRecord::PostgreSQLTestCase
  self.use_transactional_tests = false

  def setup
    super
    @connection = ActiveRecord::Base.lease_connection
    @connection.connect!
  end

  def teardown
    @connection.exit_pipeline_mode if @connection.pipeline_active?
    ActiveRecord::Base.connection_pool.release_connection
    super
  end

  def test_select_all_returns_a_pending_future
    future = @connection.select_all("SELECT 1 AS n", "TEST", pipeline: true)

    assert_kind_of ActiveRecord::FutureResult, future
    assert_predicate future, :pending?
    assert_predicate @connection, :pipeline_active?
    assert_equal [{ "n" => 1 }], future.result.to_a
    assert_not_predicate future, :pending?
  end

  def test_select_variants_return_promises
    row = @connection.select_one("SELECT 1 AS n, 2 AS m", "TEST", pipeline: true)
    value = @connection.select_value("SELECT 42", "TEST", pipeline: true)
    rows = @connection.select_rows("SELECT 1, 2 UNION ALL SELECT 3, 4", "TEST", pipeline: true)

    assert row.is_a?(ActiveRecord::Promise)
    assert value.is_a?(ActiveRecord::Promise)
    assert rows.is_a?(ActiveRecord::Promise)
    assert_equal({ "n" => 1, "m" => 2 }, row.value)
    assert_equal 42, value.value
    assert_equal [[1, 2], [3, 4]], rows.value
  end

  def test_pipeline_promise_chains
    promise = @connection.select_value("SELECT 7", "TEST", pipeline: true).then { |value| value * 6 }

    assert promise.is_a?(ActiveRecord::Promise)
    assert_equal 42, promise.value
  end

  def test_multiple_queries_share_a_pipeline_batch
    first = @connection.select_all("SELECT 1 AS n", "TEST", pipeline: true)
    second = @connection.select_all("SELECT 2 AS n", "TEST", pipeline: true)

    assert_predicate first, :pending?
    assert_predicate second, :pending?

    assert_equal [{ "n" => 2 }], second.result.to_a
    assert_not_predicate first, :pending?
    assert_equal [{ "n" => 1 }], first.result.to_a
  end

  def test_query_error_is_deferred_until_observation
    future = @connection.select_all(
      "SELECT * FROM nonexistent_pipeline_table",
      "TEST",
      pipeline: true
    )

    assert_kind_of ActiveRecord::FutureResult, future
    assert_raises(ActiveRecord::StatementInvalid) { future.result }
  end

  def test_pipeline_query_uses_the_current_transaction
    @connection.execute("CREATE TEMPORARY TABLE pipeline_transaction_test (value integer)")

    @connection.transaction do
      @connection.execute("INSERT INTO pipeline_transaction_test VALUES (1)")
      future = @connection.select_value(
        "SELECT COUNT(*) FROM pipeline_transaction_test",
        "TEST",
        pipeline: true
      )

      assert future.is_a?(ActiveRecord::Promise)
      assert_equal 1, future.value
      raise ActiveRecord::Rollback
    end

    assert_equal 0, @connection.select_value("SELECT COUNT(*) FROM pipeline_transaction_test")
  ensure
    @connection.execute("DROP TABLE IF EXISTS pipeline_transaction_test") if @connection.connected?
  end

  def test_query_cache_hit_returns_a_complete_future
    @connection.enable_query_cache!
    @connection.select_all("SELECT 1 AS n")

    future = nil
    events = capture_notifications("sql.active_record") do
      future = @connection.select_all("SELECT 1 AS n", pipeline: true)
    end

    assert_kind_of ActiveRecord::FutureResult::Complete, future
    assert_not_predicate future, :pending?
    assert_equal [{ "n" => 1 }], future.result.to_a
    event = events.find { |candidate| candidate.payload[:cached] }
    assert_not event.payload.key?(:pipelined)
  ensure
    @connection.disable_query_cache!
  end

  def test_pipeline_query_does_not_populate_the_query_cache
    @connection.enable_query_cache!
    @connection.clear_query_cache

    future = @connection.select_all("SELECT 1 AS n", "TEST", pipeline: true)

    assert_equal [{ "n" => 1 }], future.result.to_a
    assert_predicate @connection.query_cache, :empty?
  ensure
    @connection.disable_query_cache!
  end

  def test_synchronous_result_access_settles_pending_pipeline_queries
    first = @connection.select_all("SELECT 1 AS n", "TEST", pipeline: true)
    second = @connection.select_all("SELECT 2 AS n", "TEST", pipeline: true)

    assert_equal 3, @connection.select_value("SELECT 3")

    assert_not_predicate first, :pending?
    assert_not_predicate second, :pending?
    assert_equal [{ "n" => 1 }], first.result.to_a
    assert_equal [{ "n" => 2 }], second.result.to_a
  end

  def test_pipeline_can_restart_after_a_synchronous_barrier
    first = @connection.select_value("SELECT 1", "TEST", pipeline: true)
    second = @connection.select_value("SELECT 2", "TEST", pipeline: true)

    assert_equal 3, @connection.select_value("SELECT 3")
    assert_equal 1, first.value
    assert_equal 2, second.value

    third = @connection.select_value("SELECT 4", "TEST", pipeline: true)
    fourth = @connection.select_value("SELECT 5", "TEST", pipeline: true)

    assert_equal 4, third.value
    assert_equal 5, fourth.value
  end

  def test_explicit_pipeline_rejects_an_ineligible_query
    pending = @connection.select_all("SELECT 1 AS n", "TEST", pipeline: true)

    assert_raises(ArgumentError) do
      @connection.select_all("SELECT 2; SELECT 3", "TEST", pipeline: true)
    end

    assert_predicate pending, :pending?
    assert_equal [{ "n" => 1 }], pending.result.to_a
  end

  def test_explicit_pipeline_rejects_prepared_arel
    skip unless @connection.prepared_statements

    assert_raises(ArgumentError) do
      @connection.select_all(Post.where(id: -1).arel, "TEST", pipeline: true)
    end
  end

  def test_explicit_pipeline_conservatively_rejects_unbound_semicolons
    ["SELECT 1;", "SELECT ';' AS value"].each do |sql|
      assert_raises(ArgumentError) do
        @connection.select_all(sql, "TEST", pipeline: true)
      end
    end
  end

  def test_bound_multi_command_query_uses_extended_query_error
    bind = ActiveRecord::Relation::QueryAttribute.new(nil, 1, ActiveRecord::Type::Value.new)
    future = @connection.select_all(
      "SELECT $1::integer; SELECT 2",
      "TEST",
      [bind],
      pipeline: true
    )

    error = assert_raises(ActiveRecord::StatementInvalid) { future.result }
    assert_match(/multiple commands/, error.message)
  end

  def test_explicit_pipeline_falls_back_on_an_unsupported_connection
    future, promise = @connection.stub(:supports_pipeline?, false) do
      [
        @connection.select_all("SELECT 1", "TEST", pipeline: true),
        @connection.select_value("SELECT 2", "TEST", pipeline: true),
      ]
    end

    assert_instance_of ActiveRecord::FutureResult::Complete, future
    assert_equal [[1]], future.result.rows
    assert promise.is_a?(ActiveRecord::Promise)
    assert_equal 2, promise.value
    assert_not_predicate @connection, :pipeline_active?
  end

  def test_async_foreground_fallback_settles_an_active_pipeline
    skip unless @connection.async_enabled?

    first = @connection.select_all("SELECT 1 AS n", "TEST", pipeline: true)
    second = @connection.select_all("SELECT 2 AS n", "TEST", pipeline: true)

    async_future = @connection.pool.stub(:schedule_query, proc { }) do
      @connection.select_all("SELECT 3 AS n", "TEST", async: true)
    end

    assert_equal [{ "n" => 3 }], async_future.result.to_a
    assert_equal [{ "n" => 1 }], first.result.to_a
    assert_equal [{ "n" => 2 }], second.result.to_a
  end

  def test_async_and_pipeline_execution_can_be_combined
    future = @connection.select_all(
      "SELECT 1 AS n",
      "TEST",
      async: true,
      pipeline: true
    )

    assert_kind_of ActiveRecord::FutureResult, future
    assert_equal [{ "n" => 1 }], future.result.to_a
  end
end
