# frozen_string_literal: true

module ActiveRecord
  class PredicateBuilder
    class PolymorphicArrayValue # :nodoc:
      def initialize(reflection, values)
        @reflection = reflection
        @values = values
      end

      def queries
        return [ reflection.join_foreign_key => values ] if values.empty?

        route_groups.map do |_key, (fixed_values, owner_key, ids)|
          fixed_values.merge(owner_key => ids)
        end
      end

      private
        attr_reader :reflection, :values

        def route_groups
          values.each_with_object({}) do |value, groups|
            if route = route_for(value)
              fixed_values = route.fixed_reference_values
              owner_key = route.owner_key.name
              key = [fixed_values, owner_key]
            else
              fixed_values = {}
              owner_key = reflection.join_foreign_key
              key = [fixed_values, owner_key]
            end

            group = groups[key] ||= [fixed_values, owner_key, []]
            group.last << convert_to_id(value, route)
          end
        end

        def route_for(value)
          if value.is_a?(Base) || value.is_a?(Relation)
            reflection.association_router.route_for_referenced(value)
          end
        end

        def convert_to_id(value, route)
          if value.is_a?(Base)
            route.target_key.value_of(value)
          elsif value.is_a?(Relation)
            value.select(route.target_key.name)
          else
            value
          end
        end
    end
  end
end
