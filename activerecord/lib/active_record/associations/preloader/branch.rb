# frozen_string_literal: true

module ActiveRecord
  module Associations
    class Preloader
      class Branch # :nodoc:
        attr_reader :association, :children, :parent
        attr_reader :scope, :associate_by_default
        attr_writer :preloaded_records

        def initialize(association:, children:, parent:, associate_by_default:, scope:)
          @association = if association
            begin
              @association = association.to_sym
            rescue NoMethodError
              raise ArgumentError, "Association names must be Symbol or String, got: #{association.class.name}"
            end
          end
          @parent = parent
          @scope = scope
          @associate_by_default = associate_by_default

          @children = build_children(children)
          @loaders = nil
        end

        def future_classes
          (immediate_future_classes + children.flat_map(&:future_classes)).uniq
        end

        def immediate_future_classes
          if parent.done?
            loaders.flat_map(&:future_classes).uniq
          else
            likely_reflections.reject(&:polymorphic?).flat_map do |reflection|
              reflection.
                chain.
                map(&:klass)
            end.uniq
          end
        end

        def target_classes
          if done?
            preloaded_records.map(&:klass).uniq
          elsif parent.done?
            loaders.map(&:klass).uniq
          else
            likely_reflections.reject(&:polymorphic?).map(&:klass).uniq
          end
        end

        def likely_reflections
          parent_classes = parent.target_classes
          parent_classes.filter_map do |parent_klass|
            parent_klass._reflect_on_association(@association)
          end
        end

        def root?
          parent.nil?
        end

        def source_records
          @parent.preloaded_records
        end

        def preloaded_records
          @preloaded_records ||= loaders.flat_map(&:preloaded_records)
        end

        def done?
          root? || (@loaders && @loaders.all?(&:run?))
        end

        def runnable_loaders
          loaders.flat_map(&:runnable_loaders).reject(&:run?)
        end

        def grouped_records
          h = {}
          polymorphic_parent = !root? && parent.polymorphic?
          source_records.each do |record|
            reflection = record.class._reflect_on_association(association)
            next if polymorphic_parent && !reflection || !record.association(association).klass
            (h[reflection] ||= []) << record
          end
          h
        end

        def preloaders_for_reflection(reflection, reflection_records)
          static_route = if reflection.static_association_route?
            reflection.association_route_for_origin(reflection_records.first)
          end
          if static_route && !(reflection.scope && reflection.scope.arity != 0) &&
              !(static_route.destination_scope && static_route.destination_scope.arity != 0)
            reflection_records.group_by do |record|
              record.association(association).klass
            end.map do |destination_class, records|
              preloader_for(reflection).new(
                destination_class,
                records,
                reflection,
                scope,
                nil,
                associate_by_default,
                association_route: static_route
              )
            end
          else
            # Instance-dependent Reflection and route scopes may differ for each
            # origin, so group only routes whose evaluated scopes are equivalent.
            groups = reflection_records.each_with_object({}) do |record, result|
              route = reflection.association_route_for_origin(record)
              destination_class = route.destination_class

              reflection_scope = if reflection.scope && reflection.scope.arity != 0
                reflection.join_scopes(
                  destination_class.arel_table,
                  destination_class.predicate_builder,
                  destination_class,
                  record
                ).inject(&:merge!)
              end
              route_scope = if route.destination_scope && route.destination_scope.arity != 0
                route.apply_destination_scope(destination_class.unscoped, record)
              end

              key = if reflection_scope || route_scope
                resolved_scopes = []
                resolved_scopes << reflection_scope if reflection_scope
                resolved_scopes << route_scope if route_scope
                [
                  route,
                  *resolved_scopes.flat_map do |resolved_scope|
                    [
                      resolved_scope.table_name,
                      resolved_scope.model.connection_specification_name,
                      resolved_scope.values_for_queries,
                    ]
                  end,
                ]
              else
                route
              end

              group = result[key] ||= [destination_class, route, reflection_scope, route_scope, []]
              group.last << record
            end

            groups.values.map do |destination_class, route, reflection_scope, route_scope, records|
              preloader_for(reflection).new(
                destination_class,
                records,
                reflection,
                scope,
                reflection_scope,
                associate_by_default,
                route_scope: route_scope,
                association_route: route
              )
            end
          end
        end

        def polymorphic?
          return false if root?
          return @polymorphic if defined?(@polymorphic)

          @polymorphic = source_records.any? do |record|
            reflection = record.class._reflect_on_association(association)
            reflection && reflection.options[:polymorphic]
          end
        end

        def loaders
          @loaders ||=
            grouped_records.flat_map do |reflection, reflection_records|
              Deprecation.guard(reflection) { "referenced in query to preload records" }
              preloaders_for_reflection(reflection, reflection_records)
            end
        end

        private
          def build_children(children)
            Array.wrap(children).flat_map { |association|
              Array(association).flat_map { |parent, child|
                Branch.new(
                  parent: self,
                  association: parent,
                  children: child,
                  associate_by_default: associate_by_default,
                  scope: scope
                )
              }
            }
          end

          # Returns a class containing the logic needed to load preload the data
          # and attach it to a relation. The class returned implements a `run` method
          # that accepts a preloader.
          def preloader_for(reflection)
            if reflection.options[:through]
              ThroughAssociation
            else
              Association
            end
          end
      end
    end
  end
end
