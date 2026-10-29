# frozen_string_literal: true

module Arel # :nodoc: all
  module Nodes
    class CompositeIn < Node
      UNSERIALIZABLE = Object.new.freeze
      private_constant :UNSERIALIZABLE

      attr_reader :attributes, :types, :type

      def initialize(attributes, tuples, type, types: attributes.map(&:type_caster))
        @attributes = attributes
        @types = types
        @columns = types.each_with_index.map do |cast_type, index|
          column = tuples.map { |tuple| tuple[index] }

          if cast_type.serialized?
            column.map! do |value|
              value_for_database = cast_type.serialize(value)
              cast_type.serializable?(value) ? value_for_database : UNSERIALIZABLE
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
            complete &&= !column.include?(nil) && !column.include?(UNSERIALIZABLE)
            next column
          end

          column.map do |raw_value|
            if type.serializable?(raw_value)
              value = type.serialize(raw_value)
              complete = false if value.nil?
              value
            else
              complete = false
              UNSERIALIZABLE
            end
          end
        end

        return casted_columns, nil if complete

        tested_for_null = types.map { |type| type.respond_to?(:subtype) || type.respond_to?(:normalizer) }
        rows = []
        null_tuples = []

        casted_columns.first.each_index do |row|
          tuple = casted_columns.map { |column| column[row] }
          next if tuple.include?(UNSERIALIZABLE)

          if tuple.each_with_index.any? { |value, index| value.nil? && tested_for_null[index] }
            null_tuples << tuple
          else
            rows << tuple
          end
        end

        casted_columns = rows.empty? ? attributes.map { [] } : rows.transpose
        return casted_columns, nil if null_tuples.empty?

        [casted_columns, null_tuples, tested_for_null]
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
