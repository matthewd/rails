# frozen_string_literal: true

module ActiveRecord
  module Associations
    # = Active Record Belongs To Association
    class BelongsToAssociation < SingularAssociation # :nodoc:
      attr_reader :foreign_key, :foreign_type

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

      def association_route(record = nil)
        reflection.association_route_for_origin(owner, record ? record.class : klass)
      end

      def handle_dependency
        return unless load_target

        case options[:dependent]
        when :destroy
          raise ActiveRecord::Rollback unless target.destroy
        when :destroy_async
          route = association_route(target)
          destination_key = route.destination_key
          ids = destroy_association_async_ids(route)
          association_class = if reflection.polymorphic?
            owner.public_send(foreign_type)
          else
            klass
          end

          enqueue_destroy_association(
            owner_model_name: owner.class.to_s,
            owner_id: owner.id,
            association_class: association_class.to_s,
            association_ids: route.destination_key.composite? ? [ids] : ids,
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
        if reflection.polymorphic?
          model_type_was = owner.attribute_before_last_save(foreign_type)
          model_was = owner.class.polymorphic_class_for(model_type_was) if model_type_was
        else
          model_was = klass
        end
        values = foreign_key.map { |key| owner.attribute_before_last_save(key) }
        foreign_key_was = foreign_key.composite? ? (values if values.all?) : values.first

        if foreign_key_was && model_was < ActiveRecord::Base
          route = reflection.association_route_for_origin(owner, model_was)
          values = route.origin_key.map { |key| owner.attribute_before_last_save(key) }
          origin_key_was = route.origin_key.composite? ? values : values.first
          update_counters_via_scope(model_was, origin_key_was, -1, route)
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
        # Match the target selected for synchronous destruction. Fall back to
        # owner values when a partial select omitted a physical reference key.
        def destroy_association_async_ids(route)
          route.each_match.map do |origin_column, destination_column|
            origin_value = owner.read_attribute(origin_column)
            if target.has_attribute?(destination_column)
              destination_value = target.attribute_in_database(destination_column)
              if !destination_value.nil? || !route.reference_destination_key.include?(destination_column)
                destination_value
              else
                origin_value
              end
            else
              origin_value
            end
          end
        end

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

        def update_counters_via_scope(klass, values, by, route)
          scope = klass.all_queries_scope.where!(route.destination_key.where_hash(values))
          scope.update_counters(reflection.counter_cache_column => by, touch: reflection.options[:touch])
        end

        def find_target?
          !loaded? && foreign_key_present? && klass
        end

        def require_counter_update?
          reflection.counter_cache_column && owner.persisted?
        end

        def replace_keys(record, force: false)
          reference_key = @foreign_key
          target_key = record ? association_route(record).reference_destination_key : ActiveRecord::Key.for(nil)
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
