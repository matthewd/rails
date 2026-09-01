# frozen_string_literal: true

module ActiveRecord
  class PredicateBuilder
    class AssociationQueryValue # :nodoc:
      def initialize(reflection, value)
        @reflection = reflection
        @value = value
      end

      def queries
        key = association_route.owner_key
        id_list = ids
        id_list = id_list.pluck(primary_key) if key.composite? && id_list.is_a?(Relation)

        key.where_clauses(id_list)
      end

      private
        attr_reader :reflection, :value

        def ids
          case value
          when Relation
            relation = value
            relation = relation.select(primary_key) if select_clause?
            fixed_values = association_route.target_fixed_values.reject do |column, _|
              relation.where_values_hash.has_key?(column)
            end
            relation = relation.where(fixed_values) unless fixed_values.empty?
            relation
          when Array
            value.map { |v| convert_to_id(v) }
          else
            [convert_to_id(value)]
          end
        end

        def primary_key
          association_route.target_key.name
        end

        def association_route
          @association_route ||= reflection.association_route
        end

        def select_clause?
          value.select_values.empty?
        end

        def convert_to_id(value)
          if primary_key.is_a?(Array)
            primary_key.map do |attribute|
              next nil if value.nil?

              if attribute == "id"
                value.id_value
              else
                value.public_send(attribute)
              end
            end
          elsif value.respond_to?(primary_key)
            value.public_send(primary_key)
          else
            value
          end
        end
    end
  end
end
