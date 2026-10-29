# frozen_string_literal: true

# :markup: markdown

module ActiveRecord
  module ModelSchema
    class SchemaContext # :nodoc:
      # Attributes owns a model's attribute-derived state: attribute
      # defaults, attribute types, and column defaults.
      class Attributes # :nodoc:
        # The attributes that records loaded from the database fall back to
        # defaults for when the row doesn't have them: the primary key, and
        # attributes that aren't database columns. Other missing columns stay
        # uninitialized.
        class DatabaseDefaultKeys # :nodoc:
          def initialize(primary_key, columns_hash)
            @primary_key = primary_key
            @columns_hash = columns_hash
          end

          def include?(name)
            @primary_key.include?(name) || !@columns_hash.key?(name)
          end
        end

        attr_reader :defaults, :types, :builder

        # Built up front, since shareable instances are frozen along with MappedTable.
        def initialize(mapped_table, defaults)
          @defaults = defaults
          @types = defaults.cast_types.tap do |hash|
            hash.default = ActiveModel::Type.default_value
          end
          default_keys = DatabaseDefaultKeys.new(mapped_table.model_primary_key_definition, mapped_table.columns_hash)
          @builder = ActiveModel::AttributeSet::Builder.new(@types, defaults, default_keys)
          @column_defaults = defaults.to_hash.freeze if defaults.read_only?
        end

        # Lazy otherwise, since evaluating them calls proc defaults.
        def column_defaults
          @column_defaults ||= defaults.deep_dup.to_hash.freeze
        end
      end
    end
  end
end
