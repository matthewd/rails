# frozen_string_literal: true

require "cases/helper"
require "models/post"

module ActiveRecord
  class QueryIntentTest < ActiveRecord::TestCase
    test "finalized intents cannot be delivered or reset" do
      connection = Post.lease_connection
      intent = build_intent(connection)

      intent.execute!
      intent.cast_result

      assert_predicate intent, :finalized?
      assert_raises(ActiveRecord::ConnectionAdapters::QueryIntent::FinalizedError) do
        intent.deliver_result(nil)
      end
      assert_raises(ActiveRecord::ConnectionAdapters::QueryIntent::FinalizedError) do
        intent.deliver_failure(ActiveRecord::StatementInvalid.new("boom"))
      end
      assert_raises(ActiveRecord::ConnectionAdapters::QueryIntent::FinalizedError) do
        intent.reset_for_retry
      end
    end

    test "warning collection failures are delivered through the intent" do
      connection = Post.lease_connection
      singleton_class = connection.singleton_class

      singleton_class.define_method(:collect_warnings) do |_result|
        raise ActiveRecord::StatementInvalid, "warning collection failed"
      end

      intent = build_intent(connection)
      intent.execute!

      assert_predicate intent, :raw_result_available?
      error = assert_raises(ActiveRecord::StatementInvalid) do
        intent.cast_result
      end
      assert_equal "warning collection failed", error.message
    ensure
      singleton_class.remove_method(:collect_warnings) if singleton_class&.instance_methods(false)&.include?(:collect_warnings)
    end

    test "delivered outcomes dirty materialized transactions" do
      connection = Post.lease_connection
      dirty_count = 0
      singleton_class = connection.singleton_class

      singleton_class.define_method(:dirty_current_transaction) do
        dirty_count += 1
      end

      build_intent(connection, materialize_transactions: true).deliver_result(nil)
      build_intent(connection, materialize_transactions: true).deliver_failure(ActiveRecord::StatementInvalid.new("boom"))
      build_intent(connection, materialize_transactions: false).deliver_result(nil)
      build_intent(connection, materialize_transactions: false).deliver_failure(ActiveRecord::StatementInvalid.new("boom"))

      assert_equal 2, dirty_count
    ensure
      singleton_class.remove_method(:dirty_current_transaction) if singleton_class&.instance_methods(false)&.include?(:dirty_current_transaction)
    end

    test "retriable uses allow_retry reader" do
      connection = Post.lease_connection
      intent = build_intent(connection, allow_retry: false)
      intent.define_singleton_method(:allow_retry) { true }

      intent.retry_budget = ActiveRecord::ConnectionAdapters::RetryBudget.new(
        retries: 1, deadline: nil, reconnectable: true
      )

      assert_predicate intent, :retriable?
    end

    test "retry re-enters execute_intent" do
      connection = Post.lease_connection
      intent = build_intent(connection, allow_retry: true)
      singleton_class = class << connection; self; end
      execute_intent_calls = 0
      perform_query_calls = 0
      original_execute_intent = connection.method(:execute_intent)
      original_perform_query = connection.method(:perform_query)

      singleton_class.define_method(:backoff) { |_| }
      singleton_class.define_method(:execute_intent) do |retry_intent|
        execute_intent_calls += 1
        original_execute_intent.call(retry_intent)
      end
      singleton_class.define_method(:perform_query) do |raw_connection, retry_intent|
        perform_query_calls += 1
        if perform_query_calls == 1
          raise ActiveRecord::LockWaitTimeout.new("lock wait timeout")
        else
          original_perform_query.call(raw_connection, retry_intent)
        end
      end

      intent.execute!
      intent.cast_result

      assert_equal 2, execute_intent_calls
    ensure
      singleton_class&.remove_method(:backoff) if singleton_class&.method_defined?(:backoff)
      singleton_class&.remove_method(:execute_intent) if singleton_class&.method_defined?(:execute_intent)
      singleton_class&.remove_method(:perform_query) if singleton_class&.method_defined?(:perform_query)
    end

    test "retryable failures dirty transactions only after final delivery" do
      connection = Post.lease_connection
      intent = build_intent(connection, allow_retry: true, materialize_transactions: true)
      singleton_class = class << connection; self; end
      dirty_count = 0
      dirty_count_before_retry = nil
      perform_query_calls = 0
      original_perform_query = connection.method(:perform_query)

      singleton_class.define_method(:backoff) { |_| }
      singleton_class.define_method(:dirty_current_transaction) do
        dirty_count += 1
      end
      singleton_class.define_method(:perform_query) do |raw_connection, retry_intent|
        perform_query_calls += 1
        if perform_query_calls == 1
          raise ActiveRecord::LockWaitTimeout.new("lock wait timeout")
        else
          dirty_count_before_retry = dirty_count
          original_perform_query.call(raw_connection, retry_intent)
        end
      end

      intent.execute!
      intent.cast_result

      assert_equal 0, dirty_count_before_retry
      assert_equal 1, dirty_count
    ensure
      singleton_class&.remove_method(:backoff) if singleton_class&.method_defined?(:backoff)
      singleton_class&.remove_method(:dirty_current_transaction) if singleton_class&.method_defined?(:dirty_current_transaction)
      singleton_class&.remove_method(:perform_query) if singleton_class&.method_defined?(:perform_query)
    end

    private
      def build_intent(connection, allow_retry: false, materialize_transactions: false)
        ActiveRecord::ConnectionAdapters::QueryIntent.new(
          adapter: connection,
          raw_sql: "SELECT 1",
          name: "SQL",
          allow_retry: allow_retry,
          materialize_transactions: materialize_transactions,
          batch: true
        )
      end
  end
end
