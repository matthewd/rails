# frozen_string_literal: true

require_relative "../helper"
require "arel/collectors/substitute_binds"
require "arel/collectors/sql_string"

module Arel
  module Collectors
    class TestSubstituteBindCollector < Arel::Test
      setup do
        @conn = FakeRecord::Base.new
        @visitor = Visitors::ToSql.new @conn.lease_connection
      end

      def ast_with_binds
        table = Table.new(name: :users)
        manager = Arel::SelectManager.new table
        manager.where(table[:age].eq(Nodes::BindParam.new("hello")))
        manager.where(table[:name].eq(Nodes::BindParam.new("world")))
        manager.ast
      end

      def compile(node, quoter)
        collector = Collectors::SubstituteBinds.new(quoter, Collectors::SQLString.new)
        @visitor.accept(node, collector).value
      end

      def test_compile
        quoter = Object.new
        def quoter.quote(val)
          val.to_s
        end
        sql = compile(ast_with_binds, quoter)
        assert_equal 'SELECT FROM "users" WHERE "users"."age" = hello AND "users"."name" = world', sql
      end

      def test_quoting_is_delegated_to_quoter
        quoter = Object.new
        def quoter.quote(val)
          val.inspect
        end
        sql = compile(ast_with_binds, quoter)
        assert_equal 'SELECT FROM "users" WHERE "users"."age" = "hello" AND "users"."name" = "world"', sql
      end

      def test_add_bind_rows_quotes_values_without_building_binds
        quoter = Object.new
        def quoter.quote(val)
          val.inspect
        end
        collector = Collectors::SubstituteBinds.new(quoter, Collectors::SQLString.new)
        unused = -> value { raise "bind built for #{value.inspect}" }

        collector.add_bind("before") { raise "unused" }
        collector << " AND (a, b) IN "
        collector.add_bind_rows([[1, 2], %w[x y]], [unused, unused]) { raise "unused" }
        collector << " AND y = "
        collector.add_bind("after") { raise "unused" }

        assert_equal '"before" AND (a, b) IN (1, "x"), (2, "y") AND y = "after"', collector.value
      end
    end
  end
end
