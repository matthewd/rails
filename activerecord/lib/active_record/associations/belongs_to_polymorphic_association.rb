# frozen_string_literal: true

module ActiveRecord
  module Associations
    # = Active Record Belongs To Polymorphic Association
    class BelongsToPolymorphicAssociation < BelongsToAssociation # :nodoc:
      def klass
        stored_type = owner.read_attribute(foreign_type)
        if !@association_route || @association_route.fixed_reference_values[foreign_type] != stored_type
          @association_route = reflection.association_route_from_reference(owner)
        end
        @association_route&.destination_class
      end

      def target_changed?
        super || owner.attribute_changed?(foreign_type)
      end

      def target_previously_changed?
        super || owner.attribute_previously_changed?(foreign_type)
      end

      def saved_change_to_target?
        super || owner.saved_change_to_attribute?(foreign_type)
      end

      def stale_target?
        if super
          @association_route = nil
          true
        else
          false
        end
      end

      private
        def replace_keys(record, force: false, route: nil)
          route ||= record ? association_route(record) : association_route_if_resolvable
          super(record, force: force, route: route)

          fixed_values = route&.fixed_reference_values || {}
          columns = [foreign_type, *fixed_values.keys].uniq
          columns.each do |column|
            value = record ? fixed_values[column] : nil
            if force || owner.read_attribute(column) != value
              owner.write_attribute(column, value)
            end
          end
        end

        def inverse_reflection_for(record)
          reflection.polymorphic_inverse_of(record.class)
        end

        def raise_on_type_mismatch!(record)
          # A polymorphic association cannot have a type mismatch, by definition
        end

        def stale_state
          if foreign_key = super
            [foreign_key, owner.read_attribute(foreign_type)]
          end
        end
    end
  end
end
