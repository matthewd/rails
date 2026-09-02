# frozen_string_literal: true

module ActiveRecord
  module Associations
    # = Active Record Belongs To Association
    class BelongsToAssociation < SingularAssociation # :nodoc:
      attr_reader :foreign_type

      def initialize(owner, reflection)
        super
        aliases = owner.class.attribute_aliases
        fk = reflection.foreign_key
        resolved_fk = fk.is_a?(Array) ? fk.map { |k| aliases[k] || k } : (aliases[fk] || fk)
        @foreign_key = ActiveRecord::Key.for(resolved_fk)
        if reflection.polymorphic?
          ft = reflection.foreign_type
          @foreign_type = aliases[ft] || ft
        end
      end

      def foreign_key
        route = if reflection.polymorphic?
          reflection.association_route_if_resolvable(owner)
        else
          @association_route || association_route
        end
        route ? resolved_foreign_key(route) : @foreign_key
      end

      def association_route(record = nil)
        if record
          @association_route = reflection.association_route_for_target(record)
        else
          @association_route ||= reflection.association_route_from_reference(owner)
        end
      end

      def handle_dependency
        return unless load_target

        case options[:dependent]
        when :destroy
          raise ActiveRecord::Rollback unless target.destroy
        when :destroy_async
          route = association_route
          destination_key = route.destination_key
          ids = route.origin_key.map { |column| owner.public_send(column) }
          association_class = route.destination_class

          enqueue_destroy_association(
            owner_model_name: owner.class.to_s,
            owner_id: owner.id,
            association_class: association_class.to_s,
            association_ids: route.origin_key.composite? ? [ids] : ids,
            association_primary_key_column: destination_key.name,
            ensuring_owner_was_method: options.fetch(:ensuring_owner_was, nil)
          )
        else
          target.public_send(options[:dependent])
        end
      end

      def inversed_from(record)
        replace_keys(record)
        super
      end

      def default(&block)
        writer(owner.instance_exec(&block)) if reader.nil?
      end

      def reset
        super
        @association_route = nil
        @updated = false
      end

      def updated?
        @updated
      end

      def decrement_counters
        update_counters(-1)
      end

      def increment_counters
        update_counters(1)
      end

      def decrement_counters_before_last_save
        aliases = owner.class.attribute_aliases
        old_route = reflection.association_route_from_reference(owner) do |column|
          owner.attribute_before_last_save(aliases[column] || column)
        end
        return unless old_route

        origin_key = old_route.origin_key
        values = origin_key.map do |key|
          owner.attribute_before_last_save(aliases[key] || key)
        end
        origin_key_was = origin_key.composite? ? (values if values.all?) : values.first

        if origin_key_was && old_route.destination_class < ActiveRecord::Base
          update_counters_via_scope(old_route.destination_class, origin_key_was, -1, old_route)
        end
      end

      def target_changed?
        foreign_key.any? { |fk| owner.attribute_changed?(fk) } || (!foreign_key_present? && target&.new_record?)
      end

      def target_previously_changed?
        foreign_key.any? { |fk| owner.attribute_previously_changed?(fk) }
      end

      def saved_change_to_target?
        foreign_key.any? { |fk| owner.saved_change_to_attribute?(fk) }
      end

      private
        def replace(record)
          if record
            raise_on_type_mismatch!(record)
            set_inverse_instance(record)
            @updated = true
          elsif target
            remove_inverse_instance(target)
          end

          replace_keys(record, force: true)

          self.target = record
        end

        def update_counters(by)
          if require_counter_update? && foreign_key_present?
            if target && !stale_target?
              target.increment!(reflection.counter_cache_column, by, touch: reflection.options[:touch])
            else
              route = association_route
              update_counters_via_scope(klass, route.origin_key.value_of(owner), by, route)
            end
          end
        end

        def update_counters_via_scope(klass, values, by, route = association_route)
          scope = klass.all_queries_scope.where!(route.destination_key.where_hash(values))
          scope.update_counters(reflection.counter_cache_column => by, touch: reflection.options[:touch])
        end

        def find_target?
          !loaded? && foreign_key_present? && klass
        end

        def require_counter_update?
          reflection.counter_cache_column && owner.persisted?
        end

        def replace_keys(record, force: false, route: nil)
          route ||= record ? association_route(record) : association_route_if_resolvable
          reference_key = route ? resolved_foreign_key(route) : @foreign_key
          target_key = route&.link&.reference&.target_key || ActiveRecord::Key.for(nil)
          target_key_values = record ? target_key.map { |key| record.read_attribute(key) } : []
          origin_key_values = reference_key.map { |key| owner.read_attribute(key) }

          return if !force && origin_key_values == target_key_values

          origin_primary_key = ActiveRecord::Key.for(owner.class.primary_key)

          # Preserve shared primary key columns only if another foreign key
          # column can be cleared to disassociate the record.
          preserve_origin_primary_key = record.nil? && reference_key.any? { |key| !origin_primary_key.include?(key) }

          reference_key.each_with_index do |key, index|
            next if preserve_origin_primary_key && origin_primary_key.include?(key)
            owner.write_attribute(key, target_key_values[index])
          end
        end

        def association_route_if_resolvable
          @association_route || reflection.association_route_if_resolvable(owner)
        end

        def resolved_foreign_key(route)
          return @resolved_foreign_key if @foreign_key_route.equal?(route)

          reference_key = route.link.reference.reference_key
          @foreign_key_route = route
          @resolved_foreign_key = if reference_key == @foreign_key
            @foreign_key
          else
            aliases = owner.class.attribute_aliases
            columns = reference_key.map { |key| aliases[key] || key }
            ActiveRecord::Key.for(columns.one? ? columns.first : columns)
          end
        end

        def foreign_key_present?
          foreign_key.all? { |fk| owner.read_attribute(fk) }
        end

        def invertible_for?(record)
          inverse = inverse_reflection_for(record)
          inverse && (inverse.has_one? || inverse.klass.has_many_inversing)
        end

        def stale_state
          values = foreign_key.map do |fk|
            owner.read_attribute(fk) { |n| owner.send(:missing_attribute, n, caller) }
          end
          foreign_key.composite? ? (values if values.any?) : values.first
        end
    end
  end
end
