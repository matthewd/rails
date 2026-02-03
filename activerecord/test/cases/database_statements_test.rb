# frozen_string_literal: true

require "cases/helper"

class DatabaseStatementsTest < ActiveRecord::TestCase
  def setup
    @connection = ActiveRecord::Base.lease_connection
  end

  def test_exec_insert
    result = assert_deprecated(ActiveRecord.deprecator) do
      @connection.exec_insert("INSERT INTO accounts (firm_id,credit_limit) VALUES (42,5000)", nil, [])
    end
    assert_not_nil @connection.send(:last_inserted_id, result)
  end

  def test_insert_should_return_the_inserted_id
    assert_not_nil return_the_inserted_id(method: :insert)
  end

  def test_create_should_return_the_inserted_id
    assert_not_nil return_the_inserted_id(method: :create)
  end

  def test_sql_for_insert_uses_schema_cache_for_primary_key
    skip unless @connection.supports_insert_returning?

    # Prime the schema cache with the primary key for accounts table
    @connection.schema_cache.primary_keys("accounts")

    # After priming the cache, sql_for_insert should not make additional queries
    # when determining the primary key
    assert_no_queries(include_schema: true) do
      sql, binds = @connection.send(:sql_for_insert,
        "INSERT INTO accounts (firm_id,credit_limit) VALUES (42,5000)",
        nil, # pk is nil, should use cache
        [],
        nil
      )
      assert_match(/RETURNING/, sql)
    end
  end

  private
    def return_the_inserted_id(method:)
      @connection.send(method, "INSERT INTO accounts (firm_id,credit_limit) VALUES (42,5000)")
    end
end
