# frozen_string_literal: true

# :markup: markdown

module ActiveRecord
  module ModelSchema
    class SchemaContext # :nodoc:
      # The columns of a context's table and the metadata derived from them.
      class MappedTable # :nodoc:
        attr_reader :primary_key_definition, :columns_hash, :columns, :column_names,
          :content_columns, :query_constraints_list, :composite_query_constraints_list,
          :composite_query_constraints_definition, :timestamp_attributes_for_create_in_model,
          :timestamp_attributes_for_update_in_model, :all_timestamp_attributes_in_model

        attr_accessor :attributes

        def initialize(context)
          columns_hash = context.schema_cache.columns_hash(context.table_name)
          if context.only_columns.present?
            columns_hash = columns_hash.slice(*context.only_columns)
          elsif context.ignored_columns.present?
            columns_hash = columns_hash.except(*context.ignored_columns)
          end
          @columns_hash = columns_hash.freeze
          @columns = @columns_hash.values.freeze
          @column_names = @columns.map(&:name).freeze
        end

        def resolve_model_hooks(context)
          model_class = context.model_class
          @primary_key_definition = model_class.primary_key_definition

          primary_key = model_class.primary_key
          @query_constraints_list = context.query_constraints_definition ||
            context.derive_query_constraints_list(primary_key)
          @composite_query_constraints_list = (@query_constraints_list || Array(primary_key)).freeze
          @composite_query_constraints_definition = ActiveRecord::Key.for(@composite_query_constraints_list)
          @content_columns = @columns.reject do |column|
            Array(primary_key).include?(column.name) ||
              column.name == model_class.inheritance_column ||
              column.name.end_with?("_id", "_count")
          end.freeze

          @timestamp_attributes_for_create_in_model = (model_class.timestamp_attributes_for_create & @column_names).freeze
          @timestamp_attributes_for_update_in_model = (model_class.timestamp_attributes_for_update & @column_names).freeze
          @all_timestamp_attributes_in_model = (@timestamp_attributes_for_create_in_model + @timestamp_attributes_for_update_in_model).freeze
        end

        def table_exists?
          true
        end

        # Stands in for the MappedTable of a missing or unconfigured table.
        class MissingTable # :nodoc:
          def initialize(context)
            @table_name = context.table_name
            @model_name = context.model_class.to_s
          end

          def table_exists?
            false
          end

          def columns_hash
            if @table_name
              raise ActiveRecord::StatementInvalid, "Could not find table '#{@table_name}'"
            else
              raise ActiveRecord::TableNotSpecified, "#{@model_name} has no table configured. Set one with #{@model_name}.table_name="
            end
          end
          alias_method :columns, :columns_hash
          alias_method :column_names, :columns_hash
          alias_method :content_columns, :columns_hash
          alias_method :attributes, :columns_hash
          alias_method :query_constraints_list, :columns_hash
          alias_method :composite_query_constraints_list, :columns_hash
          alias_method :composite_query_constraints_definition, :columns_hash
          alias_method :timestamp_attributes_for_create_in_model, :columns_hash
          alias_method :timestamp_attributes_for_update_in_model, :columns_hash
          alias_method :all_timestamp_attributes_in_model, :columns_hash
        end
      end
    end
  end
end
