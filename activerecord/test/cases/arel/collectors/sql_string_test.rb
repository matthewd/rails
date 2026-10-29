# frozen_string_literal: true

require_relative "../helper"

module Arel
  module Collectors
    class TestSqlString < Arel::Test
      setup do
        @conn = FakeRecord::Base.new
        @visitor = Visitors::ToSql.new @conn.lease_connection
      end

      test "compile" do
        sql = compile(ast_with_binds)
        assert_equal 'SELECT FROM "users" WHERE "users"."age" = ? AND "users"."name" = ?', sql
      end

      test "returned SQL uses UTF-8 encoding" do
        sql = compile(ast_with_binds)
        assert_equal sql.encoding, Encoding::UTF_8
      end

      test "add_bind_rows numbers placeholders in row order between other binds" do
        collector = Collectors::SQLString.new
        collector << "x = "
        collector.add_bind("before") { |i| "$#{i}" }
        collector << " AND (a, b) IN "
        collector.add_bind_rows([[1, 2], [3, 4]], nil) { |i| "$#{i}" }
        collector << " AND y = "
        collector.add_bind("after") { |i| "$#{i}" }

        assert_equal "x = $1 AND (a, b) IN ($2, $3), ($4, $5) AND y = $6", collector.value
      end

      private
        def collect(node)
          @visitor.accept(node, Collectors::SQLString.new)
        end

        def compile(node)
          collect(node).value
        end

        def ast_with_binds
          table = Table.new(name: :users)
          manager = Arel::SelectManager.new table
          manager.where(table[:age].eq(Nodes::BindParam.new("hello")))
          manager.where(table[:name].eq(Nodes::BindParam.new("world")))
          manager.ast
        end
    end
  end
end
