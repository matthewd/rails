# frozen_string_literal: true

# :markup: markdown

require "active_support/weak_key_map"
require "active_record/model_schema/schema_context/mapped_table"
require "active_record/model_schema/schema_context/attributes"

module ActiveRecord
  module ModelSchema
    # A model's schema configuration, available without consulting the database.
    class SchemaContext # :nodoc:
      attr_reader :model_class, :table_name, :only_columns, :ignored_columns, :query_constraints_definition

      def initialize(model_class)
        @model_class = model_class
        @table_name = model_class.table_name
        @only_columns = model_class.only_columns
        @ignored_columns = model_class.ignored_columns
        @query_constraints_definition = model_class.query_constraints_definition
      end

      def attributes
        # Share read-only attributes with MappedTable. Otherwise each Ractor builds its own.
        mapped_table.attributes || initialize_attributes
      end

      def columns_hash
        mapped_table.columns_hash
      end

      def columns
        mapped_table.columns
      end

      def column_names
        mapped_table.column_names
      end

      def content_columns
        mapped_table.content_columns
      end

      def query_constraints_list
        mapped_table.query_constraints_list
      end

      def derive_query_constraints_list(primary_key)
        if model_class.base_class? || primary_key != model_class.base_class.primary_key
          primary_key if primary_key.is_a?(Array)
        else
          model_class.base_class.query_constraints_definition || (primary_key if primary_key.is_a?(Array))
        end
      end

      def composite_query_constraints_list
        mapped_table.composite_query_constraints_list
      end

      def composite_query_constraints_definition
        mapped_table.composite_query_constraints_definition
      end

      def timestamp_attributes_for_create_in_model
        mapped_table.timestamp_attributes_for_create_in_model
      end

      def timestamp_attributes_for_update_in_model
        mapped_table.timestamp_attributes_for_update_in_model
      end

      def all_timestamp_attributes_in_model
        mapped_table.all_timestamp_attributes_in_model
      end

      def primary_key
        model_class.primary_key
      end

      def schema_cache
        model_class.schema_cache
      end

      def table_exists?
        table_name && schema_cache.data_source_exists?(table_name)
      end

      def has_query_constraints?
        !!query_constraints_definition
      end

      def _returning_columns_for_insert(connection)
        auto_populated_columns = columns.filter_map do |c|
          -c.name if connection.return_value_after_insert?(c)
        end

        (auto_populated_columns.empty? ? Array(primary_key) : auto_populated_columns).freeze
      end

      def _returning_columns_for_update(connection)
        columns.filter_map do |c|
          c.name if connection.return_value_after_update?(c)
        end.freeze
      end

      def cached_find_by_statement(connection, key, &block) # :nodoc:
        cache = find_by_statement_cache[connection.prepared_statements]
        cache.compute_if_absent(key) { StatementCache.create(connection, &block) }
      end

      def initialize_find_by_cache # :nodoc:
        ActiveSupport::Ractors[model_class.find_by_statement_cache_key] = { true => Concurrent::Map.new, false => Concurrent::Map.new }
      end

      def find_by_statement_cache # :nodoc:
        ActiveSupport::Ractors[model_class.find_by_statement_cache_key] || initialize_find_by_cache
      end

      def schema_loaded?
        if mapped_table = mapped_tables[self]
          return mapped_table.frozen? && mapped_table.table_exists?
        end

        ActiveSupport::Ractors.on_main(self) do
          entry = mapped_tables[self]
          !!(entry && entry.frozen? && entry.table_exists?)
        end
      end

      # The model's columns as attributes, with its attribute declarations
      # applied. Read only unless some default can't be frozen.
      def build_default_attributes # :nodoc:
        attributes = columns_hash.transform_values do |column|
          ActiveModel::Attribute.from_database(column.name, column.default, model_class.type_for_column(column))
        end
        model_class.apply_pending_attribute_modifications(attributes)

        if attributes.each_value.all?(&:freezable?)
          attributes.each_value(&:freeze)
          ActiveModel::ReadOnlyAttributeSet.new(attributes.freeze)
        else
          ActiveModel::AttributeSet.new(attributes)
        end
      end

      def load_schema!
        unless table_exists?
          return mapped_tables[self] = ActiveSupport::Ractors.make_shareable(MappedTable::MissingTable.new(self).freeze)
        end

        # Model hooks and attribute initialization can read schema metadata
        # recursively. Other callers acquire the model's schema monitor until
        # this data is frozen.
        mapped_table = mapped_tables[self] = MappedTable.new(self)
        mapped_table.resolve_model_hooks(self)
        model_class.make_pending_attribute_modifications_shareable
        attributes = Attributes.new(mapped_table, build_default_attributes)
        if attributes.defaults.read_only? && ActiveSupport::Ractors.shareable?(attributes.defaults)
          mapped_table.attributes = attributes
        else
          attributes_cache[self] = attributes
        end
        ActiveSupport::Ractors.make_shareable(mapped_table.freeze)
      end

      def clear_schema # :nodoc:
        mapped_tables.delete(self)
        attributes_cache.delete(self)
      end

      def load_schema # :nodoc:
        mapped_table = mapped_tables[self]
        return mapped_table if mapped_table&.frozen? && mapped_table.table_exists?

        mapped_table = ActiveSupport::Ractors.on_main(self) do
          model_class.load_schema(self)
          mapped_tables[self]
        end
        mapped_tables[self] = mapped_table
      end

      private
        def mapped_table
          model_class.mapped_table_for(self)
        end

        def initialize_attributes
          attributes_cache[self] ||= Attributes.new(mapped_table, build_default_attributes)
        end

        def mapped_tables
          ActiveSupport::Ractors.store_if_absent(:active_record_mapped_tables) { ActiveSupport::WeakKeyMap.new }
        end

        def attributes_cache
          ActiveSupport::Ractors.store_if_absent(:active_record_schema_attributes) { ActiveSupport::WeakKeyMap.new }
        end
    end
  end
end
