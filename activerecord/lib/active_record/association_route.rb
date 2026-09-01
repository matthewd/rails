# frozen_string_literal: true

module ActiveRecord
  # An ordered mapping between columns on the record that stores an association
  # reference and columns on the record it references.
  class KeyMapping # :nodoc:
    include Enumerable

    attr_reader :referencing_key, :referenced_key

    def self.empty
      @empty ||= new(referencing_key: nil, referenced_key: nil)
    end

    def initialize(referencing_key:, referenced_key:)
      @referencing_key = key_for(referencing_key)
      @referenced_key = key_for(referenced_key)
      if @referencing_key.length != @referenced_key.length
        raise ArgumentError, "association key mappings must have the same number of columns"
      end
      @pairs = @referencing_key.zip(@referenced_key).map!(&:freeze).freeze
      freeze
    end

    def each(&block)
      @pairs.each(&block)
    end

    def empty?
      !@referencing_key.present? && !@referenced_key.present?
    end

    def +(other)
      return other if empty?
      return self if other.empty?

      self.class.new(
        referencing_key: [*@referencing_key, *other.referencing_key],
        referenced_key: [*@referenced_key, *other.referenced_key]
      )
    end

    def from(side)
      Traversal.new(self, side)
    end

    def values_from(record, side, &reader)
      reader ||= ->(column) { record.read_attribute(column) }
      key(side).map { |column| reader.call(column) }
    end

    def write(referencing_record, referenced_record)
      each do |referencing_column, referenced_column|
        value = referenced_record.read_attribute(referenced_column)
        if referencing_record.read_attribute(referencing_column) != value
          referencing_record.write_attribute(referencing_column, value)
        end
      end
    end

    def ==(other)
      other.is_a?(KeyMapping) &&
        referencing_key == other.referencing_key &&
        referenced_key == other.referenced_key
    end
    alias_method :eql?, :==

    def hash
      [@referencing_key, @referenced_key].hash
    end

    protected
      def key(side)
        case side
        when :referencing then @referencing_key
        when :referenced then @referenced_key
        else
          raise ArgumentError, "unknown association endpoint: #{side.inspect}"
        end
      end

    private
      def key_for(key)
        key.is_a?(Key) ? key : Key.for(key)
      end

      class Traversal # :nodoc:
        include Enumerable

        attr_reader :owner_key, :target_key

        def initialize(mapping, owner_side)
          case owner_side
          when :referencing
            @owner_key = mapping.referencing_key
            @target_key = mapping.referenced_key
            @pairs = mapping
          when :referenced
            @owner_key = mapping.referenced_key
            @target_key = mapping.referencing_key
            @pairs = mapping.map { |referencing_column, referenced_column| [referenced_column, referencing_column].freeze }.freeze
          else
            raise ArgumentError, "unknown association endpoint: #{owner_side.inspect}"
          end
          freeze
        end

        def each(&block)
          @pairs.each(&block)
        end

        def values_from_owner(owner, &reader)
          reader ||= ->(column) { owner.read_attribute(column) }
          @owner_key.map { |column| reader.call(column) }
        end
      end
  end

  # The column mappings that establish and query a physical association link.
  class AssociationLink # :nodoc:
    attr_reader :reference, :constraints, :match

    def initialize(reference:, constraints: KeyMapping.empty)
      @reference = reference
      @constraints = constraints
      @match = constraints + reference
      freeze
    end

    def ==(other)
      other.is_a?(AssociationLink) &&
        reference == other.reference &&
        constraints == other.constraints
    end
    alias_method :eql?, :==

    def hash
      [@reference, @constraints].hash
    end
  end

  # Resolves the physical route used by an association reflection.
  class AssociationRouter # :nodoc:
    def initialize(reflection)
      @reflection = reflection
      @routes = Concurrent::Map.new
    end

    def route_for(associated = nil)
      if @reflection.polymorphic?
        associated_class = class_for(associated)
        unless associated_class
          raise ArgumentError, "a target is required for a polymorphic association route"
        end

        fixed_values = { @reflection.foreign_type => associated_class.polymorphic_name }
        cached_route(associated_class, fixed_values)
      elsif @reflection.type
        fixed_values = { @reflection.type => @reflection.active_record.polymorphic_name }
        cached_route(@reflection.klass, fixed_values)
      else
        cached_route(@reflection.klass, {})
      end
    end

    def route_for_referenced(record)
      if @reflection.type
        fixed_values = { @reflection.type => record.class.polymorphic_name }
        cached_route(@reflection.klass, fixed_values)
      else
        route_for(record)
      end
    end

    def resolve_reference(record, &reader)
      return route_for_referenced(record) unless @reflection.polymorphic?

      reader ||= ->(column) { record.read_attribute(column) }
      stored_type = reader.call(@reflection.foreign_type)
      return unless stored_type.present?

      associated_class = record.class.polymorphic_class_for(stored_type)
      fixed_values = { @reflection.foreign_type => stored_type }
      cached_route(associated_class, fixed_values)
    end

    def relation_route(referenced: nil)
      return if @reflection.polymorphic?

      if @reflection.type && referenced
        referenced_class = class_for(referenced)
        fixed_values = { @reflection.type => referenced_class.polymorphic_name }
        cached_route(@reflection.klass, fixed_values)
      else
        route_for
      end
    end

    def clear
      @routes.clear
    end

    private
      def cached_route(associated_class, fixed_values)
        fixed_values = fixed_values.transform_keys { |column| -column.to_s }.freeze
        key = [associated_class, fixed_values].freeze
        @routes.compute_if_absent(key) do
          @reflection.send(:build_association_route, associated_class, fixed_reference_values: fixed_values)
        end
      end

      def class_for(associated)
        case associated
        when Class
          associated
        when Relation
          associated.model
        when nil
          nil
        else
          associated.class
        end
      end
  end

  # A resolved association link, viewed from one reflection endpoint.
  class AssociationRoute # :nodoc:
    include Enumerable

    attr_reader :referencing_class, :referenced_class, :link, :owner_side,
      :fixed_reference_values, :referencing_scope, :referenced_scope

    def initialize(referencing_class:, referenced_class:, link:, owner_side:, fixed_reference_values: {},
      referencing_scope: nil, referenced_scope: nil)
      @referencing_class = referencing_class
      @referenced_class = referenced_class
      @link = link
      @owner_side = owner_side
      @fixed_reference_values = fixed_reference_values.transform_keys { |column| -column.to_s }.freeze
      @referencing_scope = referencing_scope
      @referenced_scope = referenced_scope
      @key_mapping = link.match.from(owner_side)
      @reference_mapping = link.reference.from(owner_side)
      @constraint_mapping = link.constraints.from(owner_side)
      freeze
    end

    def owner_key
      @key_mapping.owner_key
    end

    def target_key
      @key_mapping.target_key
    end

    def each(&block)
      @key_mapping.each(&block)
    end

    def values_from_owner(owner, &block)
      @key_mapping.values_from_owner(owner, &block)
    end

    def each_reference(&block)
      @reference_mapping.each(&block)
    end

    def each_constraint(&block)
      @constraint_mapping.each(&block)
    end

    def reference_owner_key
      @reference_mapping.owner_key
    end

    def reference_target_key
      @reference_mapping.target_key
    end

    def constraint_owner_key
      @constraint_mapping.owner_key
    end

    def constraint_target_key
      @constraint_mapping.target_key
    end

    def owner_fixed_values
      @owner_side == :referencing ? @fixed_reference_values : {}
    end

    def target_fixed_values
      @owner_side == :referenced ? @fixed_reference_values : {}
    end

    def target_scope
      @owner_side == :referencing ? @referenced_scope : @referencing_scope
    end

    def apply_target_scope(relation, owner = nil)
      if target_scope
        if target_scope.arity == 0
          relation.instance_exec(&target_scope) || relation
        else
          relation.instance_exec(owner, &target_scope) || relation
        end
      else
        relation
      end
    end

    def ==(other)
      other.is_a?(AssociationRoute) &&
        referencing_class == other.referencing_class &&
        referenced_class == other.referenced_class &&
        link == other.link &&
        owner_side == other.owner_side &&
        fixed_reference_values == other.fixed_reference_values &&
        referencing_scope == other.referencing_scope &&
        referenced_scope == other.referenced_scope
    end
    alias_method :eql?, :==

    def hash
      [@referencing_class, @referenced_class, @link, @owner_side,
        @fixed_reference_values, @referencing_scope, @referenced_scope].hash
    end

    def write(owner, target)
      if @owner_side == :referencing
        referencing_record = owner
        referenced_record = target
      else
        referencing_record = target
        referenced_record = owner
      end

      @link.reference.write(referencing_record, referenced_record)
      @fixed_reference_values.each do |column, value|
        referencing_record.write_attribute(column, value)
      end
    end
  end
end
