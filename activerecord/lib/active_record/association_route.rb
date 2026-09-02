# frozen_string_literal: true

module ActiveRecord
  # The physical mapping that establishes and queries an association.
  class AssociationLink # :nodoc:
    attr_reader :reference

    def initialize(reference:)
      @reference = reference
      @hash = @reference.hash
      freeze
    end

    def ==(other)
      other.is_a?(AssociationLink) && reference == other.reference
    end
    alias_method :eql?, :==

    attr_reader :hash

    def write_reference(reference_record, target_record)
      reference.each do |reference_column, target_column|
        value = target_record.read_attribute(target_column)
        if reference_record.read_attribute(reference_column) != value
          reference_record.write_attribute(reference_column, value)
        end
      end
    end
  end

  # Resolves the route used by an association reflection.
  class AssociationRouteResolver # :nodoc:
    def initialize(reflection)
      @reflection = reflection
      @routes = Concurrent::Map.new
      @destination_routes = Concurrent::Map.new
      @reference_routes = Concurrent::Map.new
      @target_routes = Concurrent::Map.new
    end

    # Polymorphic caches include their naming context because class-name storage
    # can change without rebuilding reflections.
    def route_for(destination = nil)
      if @reflection.polymorphic?
        destination_class = class_for(destination)
        unless destination_class
          raise ArgumentError, "A destination is required for a polymorphic association route"
        end

        polymorphic_name = destination_class.polymorphic_name
        routes = @destination_routes.compute_if_absent(destination_class) { Concurrent::Map.new }
        routes.compute_if_absent(polymorphic_name) do
          fixed_values = { @reflection.foreign_type => polymorphic_name }
          cached_route(destination_class, fixed_values)
        end
      elsif @reflection.type
        route_for_target_class(@reflection.active_record)
      else
        @static_route ||= cached_route(@reflection.klass, {})
      end
    end

    def route_for_target(target)
      @reflection.type ? route_for_target_class(class_for(target)) : route_for(target)
    end

    def resolve_reference(reference_record, &reader)
      return route_for_target(reference_record) unless @reflection.polymorphic?

      stored_type = if reader
        reader.call(@reflection.foreign_type)
      else
        reference_record.read_attribute(@reflection.foreign_type)
      end
      return unless stored_type.present?

      stored_type = -stored_type.to_s
      class_routes = @reference_routes.compute_if_absent(reference_record.class) { Concurrent::Map.new }
      routes = class_routes.compute_if_absent(reference_record.class.store_full_class_name) { Concurrent::Map.new }
      routes.compute_if_absent(stored_type) do
        destination_class = reference_record.class.polymorphic_class_for(stored_type)
        fixed_values = { @reflection.foreign_type => stored_type }
        cached_route(destination_class, fixed_values)
      end
    end

    def resolve_reference_if_possible(reference_record, &reader)
      resolve_reference(reference_record, &reader)
    rescue NameError => error
      stored_type = reader ? reader.call(@reflection.foreign_type) : reference_record.read_attribute(@reflection.foreign_type)
      raise unless missing_polymorphic_class?(error, stored_type)
    end

    def relation_route(target: nil)
      return if @reflection.polymorphic?

      if @reflection.type && target
        route_for_target_class(class_for(target))
      else
        route_for
      end
    end

    def static?
      !@reflection.polymorphic? && !@reflection.type
    end

    def clear
      @routes.clear
      @destination_routes.clear
      @reference_routes.clear
      @target_routes.clear
      @static_route = nil
    end

    private
      def route_for_target_class(target_class)
        polymorphic_name = target_class.polymorphic_name
        @target_routes.compute_if_absent(polymorphic_name) do
          fixed_values = { @reflection.type => polymorphic_name }
          cached_route(@reflection.klass, fixed_values)
        end
      end

      # Constant lookup may report a missing parent namespace instead of the
      # complete stored type.
      def missing_polymorphic_class?(error, stored_type)
        missing_name = error.name.to_s.delete_prefix("::")
        type_name = stored_type.to_s.delete_prefix("::")
        missing_name == type_name ||
          missing_name.end_with?("::#{type_name}") ||
          type_name.start_with?("#{missing_name}::")
      end

      def cached_route(destination_class, fixed_values)
        reference_class = @reflection.belongs_to? ? @reflection.active_record : destination_class
        aliases = reference_class.attribute_aliases
        fixed_values = fixed_values.transform_keys { |column| aliases[column.to_s] || column }
        fixed_values = AssociationRoute.normalize_fixed_values(fixed_values)
        key = [destination_class, fixed_values].freeze
        @routes.compute_if_absent(key) do
          build_route(destination_class, fixed_values)
        end
      end

      def build_route(destination_class, fixed_values)
        if @reflection.belongs_to?
          reference_on = :origin
          reference = Key::Mapping.new(
            reference_key: @reflection.foreign_key,
            target_key: @reflection.association_primary_key(destination_class)
          )
        else
          reference_on = :destination
          reference = Key::Mapping.new(
            reference_key: @reflection.foreign_key,
            target_key: @reflection.active_record_primary_key
          )
        end

        AssociationRoute.new(
          destination_class: destination_class,
          link: AssociationLink.new(reference: reference),
          reference_on: reference_on,
          fixed_reference_values: fixed_values
        )
      end

      def class_for(destination)
        case destination
        when Class
          destination
        when Relation
          destination.model
        when nil
          nil
        else
          destination.class
        end
      end
  end

  # A resolved association link, viewed from one reflection endpoint.
  class AssociationRoute # :nodoc:
    include Enumerable

    class << self
      # Canonical order keeps predicate and bind construction stable.
      def normalize_fixed_values(values)
        values.sort_by { |column, _| column.to_s }.to_h do |column, value|
          [-column.to_s, immutable_value(value)]
        end.freeze
      end

      private
        def immutable_value(value)
          case value
          when Array
            value.map { |item| immutable_value(item) }.freeze
          when Hash
            value.to_h { |key, item| [immutable_value(key), immutable_value(item)] }.freeze
          when String
            -value
          else
            value
          end
        end
    end

    attr_reader :destination_class, :link, :fixed_reference_values

    def initialize(destination_class:, link:, reference_on:, fixed_reference_values: {})
      @destination_class = destination_class
      @link = link
      @reference_on = reference_on
      @fixed_reference_values = self.class.normalize_fixed_values(fixed_reference_values)

      unless reference_on == :origin || reference_on == :destination
        raise ArgumentError, "Unknown association reference endpoint: #{reference_on.inspect}"
      end

      @origin_key, @destination_key, @key_pairs = orient(link.reference)
      @hash = [@destination_class, @link, @reference_on, @fixed_reference_values].hash
      freeze
    end

    attr_reader :origin_key, :destination_key

    def each(&block)
      @key_pairs.each(&block)
    end

    def values_from_origin(origin)
      @origin_key.map { |column| origin.read_attribute(column) }
    end

    def reference_on_destination?
      @reference_on == :destination
    end

    def destination_fixed_values
      reference_on_destination? ? @fixed_reference_values : {}
    end

    def ==(other)
      other.is_a?(AssociationRoute) &&
        destination_class == other.destination_class &&
        link == other.link &&
        reference_on_destination? == other.reference_on_destination? &&
        fixed_reference_values == other.fixed_reference_values
    end
    alias_method :eql?, :==

    attr_reader :hash

    def write(origin, destination)
      if reference_on_destination?
        reference_record = destination
        target_record = origin
      else
        reference_record = origin
        target_record = destination
      end

      @link.write_reference(reference_record, target_record)
      @fixed_reference_values.each do |column, value|
        if reference_record.read_attribute(column) != value
          reference_record.write_attribute(column, value)
        end
      end
    end

    private
      def orient(mapping)
        if reference_on_destination?
          origin_key = mapping.target_key
          destination_key = mapping.reference_key
        else
          origin_key = mapping.reference_key
          destination_key = mapping.target_key
        end

        pairs = origin_key.zip(destination_key).map!(&:freeze).freeze
        [origin_key, destination_key, pairs]
      end
  end
end
