# frozen_string_literal: true

require "cases/helper"

module ActiveRecord
  class StatementInvalidTest < ActiveRecord::TestCase
    def setup
      super
      @connection = ActiveRecord::Base.connection_pool.send(:new_connection)
      @connection.connect!
    end

    def teardown
      @connection.disconnect!
      super
    end

    class MockDatabaseError < StandardError
      def result
        0
      end

      def error_number
        0
      end
    end

    test "message contains no sql" do
      sql = "SELECT * FROM nonexistent_table_for_statement_invalid_test"
      translate_exception_class = @connection.method(:translate_exception_class)
      wrapper_invoked = false
      mock_database_error = ->(_error, *args, **kwargs) do
        wrapper_invoked = true
        translate_exception_class.call(MockDatabaseError.new, *args, **kwargs)
      end

      error = assert_raises(ActiveRecord::StatementInvalid) do
        @connection.stub(:translate_exception_class, mock_database_error) do
          @connection.select_all(sql)
        end
      end

      assert wrapper_invoked, "expected the database error wrapper to be invoked"
      assert_not_includes error.message, "SELECT"
    end

    test "statement is set on select" do
      sql = "SELECT * FROM nonexistent_table_for_statement_invalid_test"
      error = assert_raises(ActiveRecord::StatementInvalid) do
        @connection.select_all(sql)
      end

      assert_equal sql, error.sql
    end

    test "statement and binds are set on select" do
      sql = "SELECT * FROM nonexistent_table_for_statement_invalid_test"
      binds = [123, 456]
      error = assert_raises(ActiveRecord::StatementInvalid) do
        @connection.select_all(sql, "SQL", binds)
      end

      assert_equal sql, error.sql
      assert_equal binds, error.binds
    end
  end
end
