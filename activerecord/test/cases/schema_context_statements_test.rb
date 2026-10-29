# frozen_string_literal: true

require "cases/helper"
require "models/topic"
require "weakref"

class SchemaContextStatementsTest < ActiveRecord::TestCase
  fixtures :topics

  def test_assigning_ignored_columns_recompiles_find_statements
    model = Class.new(ActiveRecord::Base) { self.table_name = "topics" }
    assert_includes model.find(1).attributes.keys, "content"

    model.ignored_columns = ["content"]

    assert_not_includes model.find(1).attributes.keys, "content"
  end

  def test_reset_column_information_drops_find_statements_of_every_context
    model = Class.new(ActiveRecord::Base) { self.table_name = "topics" }
    other = ActiveRecord::ModelSchema::SchemaContext.new(model)
    other.find_by_statement_cache[true][:marker] = true
    assert_nil model.schema_context.find_by_statement_cache[true][:marker]

    model.reset_column_information

    assert_empty other.find_by_statement_cache[true]
  end

  def test_defining_an_sti_subclass_drops_its_parents_find_statements_of_every_context
    parent = Class.new(Class.new(ActiveRecord::Base) { self.table_name = "topics" })
    other = ActiveRecord::ModelSchema::SchemaContext.new(parent)
    other.find_by_statement_cache[true][:marker] = true

    Class.new(parent)

    assert_empty other.find_by_statement_cache[true]
  end

  # Ruby 3.3 doesn't reliably release these objects
  if RUBY_VERSION >= "3.4"
    def test_replaced_contexts_release_their_find_statements
      model = Class.new(ActiveRecord::Base) { self.table_name = "topics" }
      ref = Thread.new do
        model.find(1)
        replaced = WeakRef.new(model.schema_context)
        model.ignored_columns = ["content"]
        replaced
      end.value

      4.times { GC.start(full_mark: true, immediate_sweep: true) }
      assert_not ref.weakref_alive?, "replaced schema context was not collected"
    end
  end
end
