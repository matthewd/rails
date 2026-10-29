# frozen_string_literal: true

module Arel # :nodoc: all
  module Nodes
    class CompositeIn < Node
      attr_reader :attributes, :types, :type

      def initialize(attributes, tuples, type, types: attributes.map(&:type_caster))
        @attributes = attributes
        @types = types
        @columns = types.each_with_index.map do |cast_type, index|
          column = tuples.map { |tuple| tuple[index] }

          if cast_type.serialized?
            column.map! do |value|
              value_for_database = cast_type.serialize(value)
              value_for_database if cast_type.serializable?(value)
            end
          elsif cast_type.mutable?
            column.map! { |value| value.frozen? ? value : value.deep_dup }
          end

          column
        end
        @type = type
      end

      def tuples
        columns.transpose
      end

      def rows_count
        columns.first.size
      end

      def hash
        ivars.hash
      end

      def eql?(other)
        super || (self.class == other.class && self.ivars == other.ivars)
      end
      alias :== :eql?

      def invert
        inverted = dup
        inverted.type = type == :in ? :notin : :in
        inverted
      end

      def casted_values
        complete = true
        casted_columns = columns.each_with_index.map do |column, index|
          type = types[index]

          if type.serialized?
            complete &&= !column.include?(nil)
            next column
          end

          column.map do |raw_value|
            value = type.serialize(raw_value) if type.serializable?(raw_value)
            complete = false if value.nil?
            value
          end
        end

        return casted_columns if complete

        # Compact like HomogeneousIn, but omit whole tuples so their components
        # remain paired. Literal nils are handled by PredicateBuilder instead.
        rows = casted_columns.transpose
        rows.reject! { |tuple| tuple.any?(&:nil?) }
        rows.empty? ? attributes.map { [] } : rows.transpose
      end

      def bind_for(index, value)
        ActiveModel::Attribute.with_cast_value(attributes[index].name, value, ActiveModel::Type.default_value)
      end

      def fetch_attribute(&block)
        attributes.all? { |attribute| attribute.fetch_attribute(&block) }
      end

      protected
        attr_reader :columns
        attr_writer :type

        def ivars
          [@attributes, @types, @columns, @type]
        end
    end
  end
end
