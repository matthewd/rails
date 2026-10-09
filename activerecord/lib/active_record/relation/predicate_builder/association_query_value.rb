# frozen_string_literal: true

module ActiveRecord
  class PredicateBuilder
    class AssociationQueryValue # :nodoc:
      def initialize(reflection, value, origin_class)
        @reflection = reflection
        @value = value
        @origin_class = origin_class
      end

      def queries
        ensure_ids_are_sufficient!

        if value.nil?
          [nil_query]
        elsif value.is_a?(Array)
          queries_for_array(value)
        else
          queries_for_ids(value, *filter_keys(value))
        end
      end

      private
        attr_reader :reflection, :value

        # Values filtering on the same columns share a single predicate.
        def queries_for_array(values)
          if values.none? { |item| record_or_relation?(item) }
            return queries_for_ids(values, *filter_keys(nil))
          end

          bare_keys = nil
          keys_by_class = {}
          values.group_by do |item|
            case item
            when Base then keys_by_class[item.class] ||= filter_keys(item)
            when Relation then filter_keys(item)
            else bare_keys ||= filter_keys(item)
            end
          end.flat_map do |(key, primary_key), items|
            queries_for_ids(items, key, primary_key)
          end
        end

        # A bare id can only stand in for a single-column key; otherwise it
        # would leave columns unmatched.
        def ensure_ids_are_sufficient!
          return unless association_route_for(nil).composite?

          items = value.is_a?(Array) ? value : [value]
          if items.any? { |item| !item.nil? && !record_or_relation?(item) }
            raise ArgumentError, "#{reflection.active_record}##{reflection.name} matches on more than one column, " \
              "so it must be queried with records or relations rather than ids"
          end
        end

        # Records and relations can supply the complete match; bare ids
        # and nil only the reference.
        def filter_keys(target)
          route = association_route_for(target)
          if record_or_relation?(target)
            [route.origin_key, route.destination_key.name]
          else
            [route.reference_origin_key, route.reference_destination_key.name]
          end
        end

        def queries_for_ids(query_value, key, primary_key)
          id_list = ids(query_value, primary_key)
          id_list = id_list.pluck(primary_key) if key.composite? && id_list.is_a?(Relation)

          key.where_clauses(id_list)
        end

        def nil_query
          key = association_route_for(nil).reference_origin_key
          key.where_hash(key.map_value { nil })
        end

        def ids(query_value, primary_key)
          case query_value
          when Relation
            relation = query_value
            relation = relation.select(primary_key) if relation.select_values.empty?
            if type = reflection.type
              physical_type = relation.model.attribute_aliases[type] || type
              unless relation.where_values_hash.has_key?(physical_type)
                # Keep the declaring reflection's discriminator for relation predicates.
                relation = relation.where(type => reflection.polymorphic_name)
              end
            end
            relation
          when Array
            query_value.map { |item| convert_to_id(item, primary_key) }
          else
            [convert_to_id(query_value, primary_key)]
          end
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
          reflection.association_route(origin_class: @origin_class, destination_class: destination_class)
        end

        def record_or_relation?(target)
          target.is_a?(Base) || target.is_a?(Relation)
        end

        def convert_to_id(value, primary_key)
          if primary_key.is_a?(Array)
            primary_key.map do |attribute|
              if value.nil?
                nil
              elsif attribute == "id"
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
