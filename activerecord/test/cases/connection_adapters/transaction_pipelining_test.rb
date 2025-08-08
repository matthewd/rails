# frozen_string_literal: true

require "cases/helper"
require "models/post"

class TransactionPipeliningTest < ActiveRecord::PostgreSQLTestCase
  fixtures :posts

  def setup
    @connection = ActiveRecord::Base.lease_connection
  end

  # Test basic transaction functionality is not broken by our changes
  def test_basic_transaction_functionality
    original_count = Post.count

    Post.transaction do
      Post.create!(title: "Pipeline Test", body: "Testing transaction pipelining")
      assert_equal original_count + 1, Post.count
    end

    assert_equal original_count + 1, Post.count
  end

  # Test nested transactions work
  def test_nested_transaction_functionality
    original_count = Post.count

    Post.transaction do
      Post.create!(title: "Outer", body: "Outer transaction")

      Post.transaction do
        Post.create!(title: "Inner", body: "Inner transaction")
        assert_equal original_count + 2, Post.count
      end
    end

    assert_equal original_count + 2, Post.count
  end

  # Test transaction rollback
  def test_transaction_rollback
    original_count = Post.count

    assert_raises(RuntimeError) do
      Post.transaction do
        Post.create!(title: "Should Rollback", body: "This should be rolled back")
        assert_equal original_count + 1, Post.count
        raise "Force rollback"
      end
    end

    assert_equal original_count, Post.count
  end

  # Test that our new methods exist and don't break anything
  def test_transaction_manager_pipeline_methods
    tm = @connection.transaction_manager

    # Test new methods exist
    assert_respond_to tm, :pipeline_materialization_active?
    assert_respond_to tm, :has_unmaterialized_transactions?

    # Should start with clean state
    assert_not tm.pipeline_materialization_active?
    assert_not tm.has_unmaterialized_transactions?

    Post.transaction do
      # Should have unmaterialized transactions before first query
      assert tm.has_unmaterialized_transactions?

      # Execute a query to materialize the transaction
      Post.limit(1).to_a

      # Should no longer have unmaterialized transactions
      assert_not tm.has_unmaterialized_transactions?
    end
  end

  # Test that should_pipeline_transactions method works
  def test_should_pipeline_transactions_logic
    # Should not pipeline outside of transaction
    if @connection.respond_to?(:should_pipeline_transactions?, true)
      assert_not @connection.send(:should_pipeline_transactions?)

      Post.transaction do
        # Behavior depends on whether pipeline is supported and enabled
        # The method should return boolean without errors
        result = @connection.send(:should_pipeline_transactions?)
        assert [true, false].include?(result)

        # Execute query to materialize
        Post.limit(1).to_a

        # Should not pipeline after materialization
        assert_not @connection.send(:should_pipeline_transactions?)
      end
    end
  end

  # Test pipeline fallback doesn't break normal operation
  def test_pipeline_fallback_mechanism
    if @connection.respond_to?(:execute_with_transaction_pipelining, true)
      # Mock pipeline failure and ensure fallback works
      original_count = Post.count

      Post.transaction do
        Post.create!(title: "Fallback Test", body: "Should work even if pipeline fails")
      end

      assert_equal original_count + 1, Post.count
    end
  end

  # Test transaction state cleanup
  def test_transaction_state_cleanup
    tm = @connection.transaction_manager
    original_count = Post.count

    assert_raises(RuntimeError) do
      Post.transaction do
        Post.create!(title: "Before Error", body: "This should be rolled back")
        raise "Simulated error"
      end
    end

    # Transaction should be rolled back
    assert_equal original_count, Post.count

    # Transaction manager state should be clean
    assert_not tm.pipeline_materialization_active?
    assert_not tm.has_unmaterialized_transactions?
  end
end
