# frozen_string_literal: true

require "cases/helper"

module SchemaLoadCounter
  extend ActiveSupport::Concern

  module ClassMethods
    attr_accessor :load_schema_calls

    def load_schema!
      self.load_schema_calls ||= 0
      self.load_schema_calls += 1
      super
    end
  end
end

class SchemaLoadingTest < ActiveRecord::TestCase
  def test_building_a_schema_context_does_not_inspect_the_database
    klass = define_model { |model| model.table_name = "topics" }

    klass.schema_cache.stub(:data_source_exists?, ->(*) { flunk "Building a context inspected the database" }) do
      assert_not_predicate klass.schema_context, :schema_loaded?

      ["title", ["title", "id"], nil].each do |key|
        klass.primary_key = key
        assert_equal ActiveRecord::Key.for(key), klass.schema_context.primary_key_definition
        assert_not_predicate klass.schema_context, :schema_loaded?
      end
    end
  end

  def test_reading_an_inferred_primary_key_loads_the_schema
    klass = define_model { |model| model.table_name = "topics" }

    assert_equal "id", klass.primary_key
    assert_predicate klass.schema_context, :schema_loaded?
    assert_no_queries(include_schema: true) do
      assert_includes klass.column_names, "title"
    end
  end

  def test_content_columns_uses_inheritance_column_set_after_primary_key_resolution
    klass = define_model { |model| model.table_name = "topics" }
    assert_equal "id", klass.primary_key

    klass.inheritance_column = "author_name"

    assert_not_includes klass.content_columns.map(&:name), "author_name"
  end

  def test_inheritance_column_change_refreshes_loaded_descendant_content_columns
    klass = define_model { |model| model.table_name = "topics" }
    models = [klass, Class.new(klass)]
    models.each do |model|
      assert_includes model.content_columns.map(&:name), "author_name"
    end

    klass.inheritance_column = "author_name"

    models.each do |model|
      assert_not_includes model.content_columns.map(&:name), "author_name"
    end
  end

  def test_inheritance_column_override_can_read_columns
    klass = define_model do |model|
      model.table_name = "topics"
      def model.inheritance_column = columns_hash.key?("author_name") ? "author_name" : super
    end

    assert_equal "author_name", klass.inheritance_column
    assert_not_includes klass.content_columns.map(&:name), "author_name"
    assert_includes klass.content_columns.map(&:name), "type"
  end

  def test_loaded_schema_context_retains_its_column_metadata_after_model_configuration_changes
    klass = define_model { |model| model.table_name = "topics" }
    context = klass.schema_context
    assert_includes context.column_names, "title"

    klass.table_name = "subscribers"

    assert_includes context.column_names, "title"
    assert_not_includes klass.column_names, "title"
  end

  def test_unloaded_context_retains_its_table_after_model_configuration_changes
    klass = define_model { |model| model.table_name = "topics" }
    context = klass.schema_context
    klass.table_name = "movies"

    assert_equal "id", context.primary_key
    assert_includes context.column_names, "title"
    assert_equal "movieid", klass.primary_key
    assert_includes klass.column_names, "name"
  end

  def test_composite_primary_key_reader_override_is_supported_once_the_schema_loads
    klass = define_model do |model|
      model.table_name = "keyboards"
      def model.primary_key = ["name", "key_number"]
    end

    assert_equal ["Keys", 4], klass.instantiate("name" => "Keys", "key_number" => 4).id
  end

  def test_replaced_context_retains_its_column_filters
    klass = define_model { |model| model.table_name = "topics" }
    context = klass.schema_context
    klass.ignored_columns = ["title"]

    assert_not_includes klass.column_names, "title"
    assert_includes context.column_names, "title"
  end

  def test_cold_schema_load_while_shard_swapping_is_prohibited
    klass = define_model { |model| model.table_name = "topics" }
    context = klass.schema_context

    ActiveRecord::Base.prohibit_shard_swapping do
      assert_includes context.column_names, "title"
      assert_equal "id", klass.primary_key
    end
  end

  def test_context_specific_primary_keys_determine_the_loaded_schema
    klass = define_model { |model| model.table_name = "topics" }
    contexts = ["title", "author_name"].map do |key|
      context = ActiveRecord::ModelSchema::SchemaContext.new(klass, primary_key: key)
      ActiveSupport::Ractors.make_shareable(context)
    end

    contexts.each do |context|
      assert_not_includes context.content_columns.map(&:name), context.primary_key
      assert_equal [context.primary_key], context.composite_query_constraints_list
    end

    assert_includes contexts.first.content_columns.map(&:name), "author_name"
    assert_includes contexts.last.content_columns.map(&:name), "title"
    assert_equal "id", klass.primary_key
  end

  def test_declared_primary_key_can_be_read_before_first_table_name_assignment
    klass = Class.new(ActiveRecord::Base) { self.primary_key = "id" }
    assert_equal "id", klass.primary_key

    klass.table_name = "topics"

    assert_equal "id", klass.primary_key
    assert_includes klass.column_names, "title"
  end

  def test_declared_primary_key_can_be_read_before_table_name_change_without_connection
    with_temporary_connection_pool do
      klass = define_model do |model|
        model.table_name = "topics"
        model.primary_key = "id"
      end
      assert_equal "id", klass.primary_key
      assert_not_predicate klass.connection_pool, :connected?

      klass.table_name = "tasks"

      assert_not_predicate klass.connection_pool, :connected?
      if in_memory_db?
        klass.lease_connection.create_table :tasks do |t|
          t.datetime :starting
        end
      end

      assert_includes klass.column_names, "starting"
      assert_not_includes klass.column_names, "title"
    end
  end

  def test_failed_schema_initialization_does_not_publish_column_metadata
    klass = define_model
    context = klass.schema_context

    klass.stub(:make_pending_attribute_modifications_shareable, -> { raise "schema initialization failed" }) do
      assert_raises(RuntimeError) { klass.column_names }
    end

    assert_nil klass.current_schema_context
    assert_not_same context, klass.schema_context
    assert_includes klass.column_names, "id"
    assert_equal 1, klass.load_schema_calls
  end

  def test_failed_schema_hook_does_not_publish_the_context
    klass = define_model

    klass.stub(:load_schema!, -> { raise "schema hook failed" }) do
      assert_raises(RuntimeError) { klass.column_names }
    end

    assert_nil klass.current_schema_context
    assert_includes klass.column_names, "id"
    assert_equal 1, klass.load_schema_calls
  end

  def test_basic_model_is_loaded_once
    klass = define_model
    klass.new
    assert_equal 1, klass.load_schema_calls
  end

  def test_model_with_custom_lock_is_loaded_once
    klass = define_model do |c|
      c.table_name = :lock_without_defaults_cust
      c.locking_column = :custom_lock_version
    end
    klass.new
    assert_equal 1, klass.load_schema_calls
  end

  def test_model_with_changed_custom_lock_is_loaded_twice
    klass = define_model do |c|
      c.table_name = :lock_without_defaults_cust
    end
    klass.new
    klass.locking_column = :custom_lock_version
    klass.new
    assert_equal 2, klass.load_schema_calls
  end

  def test_schema_loading_doesnt_query_when_schema_cache_is_loaded
    with_temporary_connection_pool do
      if in_memory_db?
        # Separate connections to an in-memory database create an entirely new database,
        # with an empty schema etc, so we just stub out this schema on the fly.
        ActiveRecord::Base.connection_pool.with_connection do |connection|
          connection.create_table :tasks do |t|
            t.datetime :starting
            t.datetime :ending
          end
        end
      end

      klass = define_model do |c|
        c.table_name = :tasks
      end

      klass.connection_pool.schema_cache.load!
      klass.connection_pool.schema_cache.add("tasks")
      klass.connection_pool.disconnect!
      klass.send(:reload_schema_from_cache)


      assert_no_queries(include_schema: true) do
        klass.load_schema
      end
      assert_equal 1, klass.load_schema_calls
    end
  end

  def test_returning_column_candidates_come_from_the_loaded_schema_cache
    with_temporary_connection_pool do
      if in_memory_db?
        ActiveRecord::Base.connection_pool.with_connection do |connection|
          connection.create_table :tasks do |t|
            t.datetime :starting
            t.datetime :ending
          end
        end
      end

      klass = define_model do |c|
        c.table_name = :tasks
      end

      klass.connection_pool.schema_cache.load!
      klass.connection_pool.schema_cache.add("tasks")
      klass.connection_pool.disconnect!
      klass.send(:reload_schema_from_cache)

      mapped_table = assert_no_queries(include_schema: true) do
        klass.mapped_table_for(klass.schema_context)
      end

      assert_equal ["id"], mapped_table.insert_returning_candidates.map(&:name)
      assert_empty mapped_table.returning_columns_for_update
      assert_not_predicate klass.connection_pool, :connected?
    end
  end

  private
    def define_model
      Class.new(ActiveRecord::Base) do
        include SchemaLoadCounter
        self.table_name = :lock_without_defaults
        yield self if block_given?
      end
    end
end

class SchemaLoadingMissingTableTest < ActiveRecord::TestCase
  skip_under_ractor_proxy

  self.use_transactional_tests = false

  setup do
    @connection = ActiveRecord::Base.lease_connection
    @connection.drop_table(:late_tables, if_exists: true)
  end

  teardown do
    @connection.drop_table(:late_tables, if_exists: true)
  end

  def test_failed_schema_reads_are_retried_after_create_table
    model = define_model

    2.times { assert_raises(ActiveRecord::StatementInvalid, match: "Could not find table 'late_tables'") { model.columns } }
    assert_raises(ActiveRecord::StatementInvalid, match: "Could not find table 'late_tables'") { model.new }

    create_late_table

    assert_equal ["id", "name"], model.column_names
    assert_equal "late", model.new(name: "late").name
    record = model.create!(name: "late")
    assert_equal "late", model.find(record.id).name
  end

  def test_missing_table_inferred_key_is_inferred_again_after_create_table
    model = define_model

    2.times { assert_equal "id", model.primary_key }
    assert_raises(ActiveRecord::StatementInvalid, match: "Could not find table 'late_tables'") { model.column_names }

    create_late_table(primary_key: "code")

    assert_equal "code", model.primary_key
    assert_equal ["code", "name"], model.column_names
  end

  def test_get_primary_key_override_without_a_table_is_retried_after_create_table
    model = define_model
    def model.get_primary_key(base_name)
      "custom_#{super}"
    end

    2.times { assert_equal "custom_id", model.primary_key }

    create_late_table(primary_key: "code")

    assert_equal "custom_code", model.primary_key
    assert_includes model.column_names, "name"
  end

  private
    def define_model
      Class.new(ActiveRecord::Base) do
        def self.name = "LateTable"
        self.table_name = "late_tables"
      end
    end

    def create_late_table(**options)
      @connection.create_table(:late_tables, **options) { |t| t.string :name }
    end
end
