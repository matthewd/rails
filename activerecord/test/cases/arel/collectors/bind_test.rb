# frozen_string_literal: true

require_relative "../helper"
require "arel/collectors/bind"

module Arel
  module Collectors
    class TestBind < Arel::Test
      setup do
        @conn = FakeRecord::Base.new
        @visitor = Visitors::ToSql.new @conn.lease_connection
      end

      test "compile gathers all bind params" do
        binds = compile(ast_with_binds(["hello", "world"]))
        assert_equal ["hello", "world"], binds

        binds = compile(ast_with_binds(["hello2", "world3"]))
        assert_equal ["hello2", "world3"], binds
      end

      test "add_bind_rows gathers binds in row order between other binds" do
        collector = Collectors::Bind.new
        collector.add_bind("before")
        collector.add_bind_rows([[1, 2, 3], %w[a b c]], row_procs) { raise "unused" }
        collector.add_bind("after")

        assert_equal ["before", [:id, 1], [:name, "a"], [:id, 2], [:name, "b"], [:id, 3], [:name, "c"], "after"], collector.value
      end

      private
        def row_procs
          [-> value { [:id, value] }, -> value { [:name, value] }]
        end

        def collect(node)
          @visitor.accept(node, Collectors::Bind.new)
        end

        def compile(node)
          collect(node).value
        end

        def ast_with_binds(bvs)
          table = Table.new(name: :users)
          manager = Arel::SelectManager.new table
          manager.where(table[:age].eq(Nodes::BindParam.new(bvs.shift)))
          manager.where(table[:name].eq(Nodes::BindParam.new(bvs.shift)))
          manager.ast
        end
    end
  end
end
