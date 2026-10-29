# frozen_string_literal: true

require_relative "../helper"
require "active_model"
require "arel/collectors/bind"
require "arel/collectors/composite"

class Arel::Nodes::CompositeInTest < Arel::Test
  class RowValueVisitor < Arel::Visitors::ToSql
    private
      alias :visit_Arel_Nodes_CompositeIn :visit_composite_in_with_row_values
  end

  setup do
    Arel::Table.engine.with_connection do |connection|
      connection.visitor = RowValueVisitor.new(connection)
    end
    @table = Arel::Table.new name: :users, type_caster: fake_caster
  end

  test "in and not in" do
    expr = composite_in([[1, "Bobby"], [2, "Robert"]])

    assert_like %{
      ("users"."id", "users"."name") IN ((?, ?), (?, ?))
    }, expr.to_sql
    assert_like %{
      ("users"."id", "users"."name") NOT IN ((?, ?), (?, ?))
    }, expr.invert.to_sql
  end

  test "binds each component's cast value in tuple order with its attribute's name" do
    expr = composite_in([["1", :Bobby], ["2", :Robert]])
    _, binds = compile_with_binds(expr)

    assert_equal [["id", 1], ["name", "Bobby"], ["id", 2], ["name", "Robert"]],
      binds.map { |bind| [bind.name, bind.value_for_database] }
  end

  test "omits whole tuples that have an unserializable component" do
    expr = composite_in([[1, "Bobby"], [2**40, "Robert"], [3, "Bob"]])
    _, binds = compile_with_binds(expr)

    assert_like %{
      ("users"."id", "users"."name") IN ((?, ?), (?, ?))
    }, expr.to_sql
    assert_equal [1, "Bobby", 3, "Bob"], binds.map(&:value_for_database)
  end

  test "matches nothing when every tuple is unserializable" do
    expr = composite_in([[2**40, "Bobby"], [2**41, "Robert"]])

    assert_equal "1=0", expr.to_sql
    assert_equal "1=1", expr.invert.to_sql
  end

  test "keeps serialized nil components in the tuple" do
    expr = composite_in([[1, "Bobby"], ["one", "Robert"]])
    _, binds = compile_with_binds(expr)

    assert_like %{
      ("users"."id", "users"."name") IN ((?, ?), (?, ?))
    }, expr.to_sql
    assert_equal [1, "Bobby", nil, "Robert"], binds.map(&:value_for_database)
  end

  test "compares components with IS NULL where their type tests a bound nil that way" do
    expr = Arel::Nodes::CompositeIn.new(
      [@table[:id], @table[:nickname]], [[1, "Bobby"], [2, " "], [3, "Bob"]], :in
    )
    _, binds = compile_with_binds(expr)

    assert_like %{
      (("users"."id", "users"."nickname") IN ((?, ?), (?, ?)) OR "users"."id" = ? AND "users"."nickname" IS NULL)
    }, expr.to_sql
    assert_equal [1, "Bobby", 3, "Bob", 2], binds.map(&:value_for_database)

    assert_like %{
      NOT (("users"."id", "users"."nickname") IN ((?, ?), (?, ?)) OR "users"."id" = ? AND "users"."nickname" IS NULL)
    }, expr.invert.to_sql
  end

  test "compares only component-wise when every tuple has a component tested with IS NULL" do
    expr = Arel::Nodes::CompositeIn.new(
      [@table[:id], @table[:nickname]], [[1, " "], ["one", " "]], :in
    )
    _, binds = compile_with_binds(expr)

    assert_like %{
      ("users"."id" = ? AND "users"."nickname" IS NULL OR "users"."id" = ? AND "users"."nickname" IS NULL)
    }, expr.to_sql
    assert_equal [1, nil], binds.map(&:value_for_database)
  end

  test "is not preparable" do
    collector = Arel::Collectors::SQLString.new.tap { |c| c.preparable = true }
    compile(composite_in([[1, "Bobby"], [2, "Robert"]]), collector)

    assert_equal false, collector.preparable
  end

  test "invert" do
    expr = composite_in([[1, "Bobby"], [2, "Robert"]])

    assert_equal composite_in([[1, "Bobby"], [2, "Robert"]], :notin), expr.invert
    assert_equal expr, expr.invert.invert
  end

  test "equality and hash" do
    tuples = [[1, "Bobby"], [2, "Robert"]]
    expr = composite_in(tuples)
    same = composite_in(tuples)

    assert_equal expr, same
    assert_equal expr.hash, same.hash
    assert_not_equal expr, composite_in([[1, "Bobby"], [2, "Bob"]])
    assert_not_equal expr, composite_in(tuples, :notin)
    assert_not_equal expr, Arel::Nodes::CompositeIn.new([@table[:id], @table[:name]], tuples, :in,
      types: [INTEGER_TYPE, ActiveModel::Type::ImmutableString.new])
  end

  test "owns the storage of its values" do
    tuples = [[1, "Bobby"], [2, "Robert"]]
    expr = composite_in(tuples)
    tuples.first[0] = 3
    tuples << [4, "Bob"]

    assert_equal [[1, "Bobby"], [2, "Robert"]], expr.tuples
  end

  test "raises a serialized type's serialization errors when built" do
    type = PrefixType.new

    assert_raises(ArgumentError) do
      Arel::Nodes::CompositeIn.new([@table[:id], @table[:code]], [[1, "a"], [2, :invalid]], :in, types: [INTEGER_TYPE, type])
    end
  end

  test "omits tuples whose serialized type's value is unserializable" do
    type = PrefixType.new
    expr = Arel::Nodes::CompositeIn.new([@table[:id], @table[:code]], [[1, "a"], [2, "unserializable"]], :in, types: [INTEGER_TYPE, type])
    _, binds = compile_with_binds(expr)

    assert_equal [1, "coded:a"], binds.map(&:value_for_database)
  end

  test "compares a serialized type's nil database values with IS NULL" do
    type = PrefixType.new
    expr = Arel::Nodes::CompositeIn.new([@table[:id], @table[:code]], [[1, "a"], [2, ""], [3, "c"]], :in, types: [INTEGER_TYPE, type])

    assert_like %{
      (("users"."id", "users"."code") IN ((?, ?), (?, ?)) OR "users"."id" = ? AND "users"."code" IS NULL)
    }, expr.to_sql
  end

  test "fetch_attribute yields every attribute, and is true only when it is for every one" do
    expr = composite_in([[1, "Bobby"], [2, "Robert"]])

    fetched = []
    assert expr.fetch_attribute { |attribute| fetched << attribute }
    assert_equal [@table[:id], @table[:name]], fetched
    assert_not expr.fetch_attribute { |attribute| attribute.name == "id" }
  end

  private
    INTEGER_TYPE = ActiveModel::Type::Integer.new.freeze
    STRING_TYPE = ActiveModel::Type::String.new.freeze

    # Like a normalized attribute type: values are tested with IS NULL when
    # they serialize to nil.
    class BlankAsNilType < ActiveModel::Type::String
      def normalizer; end

      def serialize(value)
        super(value.strip.presence)
      end
    end

    BLANK_AS_NIL_TYPE = BlankAsNilType.new.freeze

    # A serialized type with a distinct database representation. Blank values
    # serialize to nil.
    class PrefixType < ActiveModel::Type::String
      def subtype
        ActiveModel::Type::String.new
      end

      def serialize(value)
        raise ArgumentError, "not a String" unless value.is_a?(::String)
        "coded:#{value}" unless value.empty?
      end

      def serializable?(value)
        value != "unserializable"
      end

      def serialized?
        true
      end
    end

    def composite_in(tuples, type = :in)
      Arel::Nodes::CompositeIn.new([@table[:id], @table[:name]], tuples, type)
    end

    def compile_with_binds(node)
      compile(node, Arel::Collectors::Composite.new(Arel::Collectors::SQLString.new, Arel::Collectors::Bind.new))
    end

    def compile(node, collector)
      Arel::Table.engine.with_connection do |connection|
        connection.visitor.accept(node, collector).value
      end
    end

    def fake_caster
      Object.new.tap do |caster|
        def caster.type_for_attribute(attr_name)
          if attr_name == "id"
            INTEGER_TYPE
          elsif attr_name == "nickname"
            BLANK_AS_NIL_TYPE
          else
            STRING_TYPE
          end
        end
      end
    end
end
