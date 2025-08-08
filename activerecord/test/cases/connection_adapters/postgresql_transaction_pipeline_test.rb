# frozen_string_literal: true

require "cases/helper"

class PostgreSQLTransactionPipelineTest < ActiveRecord::PostgreSQLTestCase
  fixtures :posts

  def setup
    skip_unless_postgresql_pipeline_supported
    @connection = ActiveRecord::Base.lease_connection
  end

  private
    def skip_unless_postgresql_pipeline_supported
      skip "PostgreSQL pipeline not supported" unless @connection&.pipeline_supported?
    end

  # Test basic transaction functionality with pipelining enabled
  def test_basic_transaction_functionality
    original_count = Post.count
    
    Post.transaction do
      Post.create!(title: "Pipeline Test", body: "Testing transaction pipelining")
      assert_equal original_count + 1, Post.count
    end
    
    assert_equal original_count + 1, Post.count
  end

  # Test that pipelining is detected when appropriate
  def test_transaction_pipelining_detection
    # Outside transaction, should not pipeline
    refute @connection.send(:should_pipeline_transactions?)
    
    Post.transaction do
      # Inside unmaterialized transaction, should pipeline (if not disabled)
      if ENV['DISABLE_PIPELINE'] != '1'
        assert @connection.send(:should_pipeline_transactions?)
      else
        refute @connection.send(:should_pipeline_transactions?)  
      end
      
      # First query should materialize the transaction
      Post.limit(1).to_a
      
      # After materialization, should not pipeline anymore
      refute @connection.send(:should_pipeline_transactions?)
    end
  end

  # Test nested transactions (savepoints) work with pipelining
  def test_nested_transaction_pipelining
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

  # Test transaction rollback works correctly with pipelining
  def test_transaction_rollback_with_pipelining
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

  # Test that transaction state is properly cleaned up after errors
  def test_transaction_state_cleanup_on_error
    original_count = Post.count
    
    assert_raises(RuntimeError) do
      Post.transaction do
        Post.create!(title: "Before Error", body: "This should be rolled back")
        raise "Simulated error"
      end
    end
    
    # Transaction should be properly rolled back
    assert_equal original_count, Post.count
    
    # Transaction manager state should be clean
    refute @connection.transaction_manager.pipeline_materialization_active?
    refute @connection.transaction_manager.has_unmaterialized_transactions?
  end

  # Test that validation errors work correctly with pipelining
  def test_validation_error_with_pipelining
    original_count = Post.count
    
    assert_raises(ActiveRecord::RecordInvalid) do
      Post.transaction do
        post = Post.new(title: "", body: "Invalid post")
        post.save!  # Should raise validation error
      end
    end
    
    assert_equal original_count, Post.count
  end

  # Test isolation levels work with pipelining
  def test_isolation_level_with_pipelining
    skip "Isolation level test requires PostgreSQL" unless @connection.supports_transaction_isolation?
    
    Post.transaction(isolation: :read_committed) do
      Post.create!(title: "Isolation Test", body: "Testing isolation levels with pipelining")
    end
  end

  # Test that the pipeline fallback mechanism works
  def test_pipeline_fallback_on_error
    # This test verifies that if pipelining fails, we fall back to normal mode
    original_count = Post.count
    
    # Force pipeline to be available but make it fail
    @connection.define_singleton_method(:pipeline_supported?) { true }
    @connection.define_singleton_method(:pipeline_active?) { false }
    
    # Override with_pipeline to simulate failure
    original_with_pipeline = @connection.method(:with_pipeline) if @connection.respond_to?(:with_pipeline)
    @connection.define_singleton_method(:with_pipeline) do |&block|
      raise StandardError.new("Simulated pipeline failure")
    end
    
    # This should still work via fallback
    Post.transaction do
      Post.create!(title: "Fallback Test", body: "Should work via fallback")
    end
    
    assert_equal original_count + 1, Post.count
  ensure
    # Restore original method if it existed
    if original_with_pipeline
      @connection.define_singleton_method(:with_pipeline, original_with_pipeline)
    end
  end

  # Test that pipelining works with prepared statements
  def test_pipelining_with_prepared_statements
    Post.transaction do
      # This should trigger pipelining with a prepared statement
      posts = Post.where("title LIKE ?", "Test%").limit(1)
      posts.to_a  # Execute the prepared statement
    end
  end

  # Test concurrent transactions work correctly
  def test_concurrent_transactions_with_pipelining
    threads = []
    results = Queue.new
    original_count = Post.count
    thread_count = 3
    
    thread_count.times do |i|
      threads << Thread.new do
        begin
          Post.transaction do
            Post.create!(title: "Thread #{i}", body: "Concurrent test")
            results << "success"
          end
        rescue => e
          results << e
        end
      end
    end
    
    threads.each(&:join)
    
    # All threads should succeed
    thread_count.times do
      result = results.pop
      assert_equal "success", result, "Expected success, got: #{result.inspect}"
    end
    
    assert_equal original_count + thread_count, Post.count
  end
end