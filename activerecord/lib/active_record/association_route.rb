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
        referencing_record.write_attribute(referencing_column, value)
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
  end

  # A resolved association link, viewed from one reflection endpoint.
  class AssociationRoute # :nodoc:
    include Enumerable

    attr_reader :referencing_class, :referenced_class, :link, :owner_side,
      :fixed_reference_values

    def initialize(referencing_class:, referenced_class:, link:, owner_side:, fixed_reference_values: {})
      @referencing_class = referencing_class
      @referenced_class = referenced_class
      @link = link
      @owner_side = owner_side
      @fixed_reference_values = fixed_reference_values.transform_keys { |column| -column.to_s }.freeze
      @key_mapping = link.match.from(owner_side)
      @reference_mapping = link.reference.from(owner_side)
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

    def reference_owner_key
      @reference_mapping.owner_key
    end

    def reference_target_key
      @reference_mapping.target_key
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
