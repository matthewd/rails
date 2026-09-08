# frozen_string_literal: true

module ActiveRecord
  class PredicateBuilder
    class AssociationQueryValue # :nodoc:
      def initialize(reflection, value, origin_model, route = nil)
        @reflection = reflection
        @value = value
        @origin_model = origin_model
        @association_route = route
      end

      def queries
        return queries_for_ids if @association_route

        if value.is_a?(Array) && value.none? { |record| record.is_a?(Base) || record.is_a?(Relation) }
          @association_route = association_route_for(nil)
          queries_for_ids
        elsif value.is_a?(Array)
          routes = value.group_by { |record| association_route_for(record) }
          if routes.empty? || routes.one?
            @association_route = routes.keys.first if routes.one?
            queries_for_ids
          else
            routes.flat_map do |route, records|
              self.class.new(reflection, records, @origin_model, route).queries
            end
          end
        else
          queries_for_ids
        end
      end

      private
        attr_reader :reflection, :value

        def queries_for_ids
          key = association_route.origin_key
          id_list = ids
          id_list = id_list.pluck(primary_key) if key.composite? && id_list.is_a?(Relation)

          key.where_clauses(id_list)
        end

        def ids
          case value
          when Relation
            relation = value
            relation = relation.select(primary_key) if select_clause?
            fixed_values = association_route.destination_fixed_values.reject do |column, _|
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
          association_route.destination_key.name
        end

        def association_route
          @association_route ||= association_route_for(value)
        end

        def association_route_for(target)
          destination_class = case target
          when Relation
            target.model
          when Base
            target.class
          else
            reflection.klass
          end
          reflection.association_route_for_origin(@origin_model, destination_class || reflection.klass)
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
