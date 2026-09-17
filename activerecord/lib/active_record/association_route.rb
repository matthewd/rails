# frozen_string_literal: true

module ActiveRecord
  # The writable reference and query-only mappings between associated records.
  class AssociationLink # :nodoc:
    attr_reader :reference, :reference_key, :constraints, :match

    def initialize(reference: nil, reference_key: reference&.reference_key, constraints: Key::Mapping.empty)
      @reference = reference
      @reference_key = reference_key
      @constraints = constraints
      @match = (constraints + reference).normalize if reference
      freeze
    end
  end

  # A resolved association link, viewed from one reflection endpoint.
  # Keys retain declared attribute names; consumers resolve aliases as needed.
  class AssociationRoute # :nodoc:
    EMPTY_FIXED_VALUES = {}.freeze
    EMPTY_KEY_PAIRS = [].freeze
    private_constant :EMPTY_FIXED_VALUES, :EMPTY_KEY_PAIRS

    attr_reader :link, :fixed_reference_values

    def initialize(link:, fixed_reference_values: {})
      @link = link
      @fixed_reference_values = fixed_reference_values.freeze
      initialize_keys
      @key_pairs = pairs_for(origin_key, destination_key)
      @reference_pairs = if link.constraints.empty?
        @key_pairs
      else
        pairs_for(reference_origin_key, reference_destination_key)
      end
      freeze
    end

    attr_reader :origin_key, :destination_key, :reference_origin_key, :reference_destination_key

    def each_match(&block)
      @key_pairs.each(&block)
    end

    def each_reference(&block)
      @reference_pairs.each(&block)
    end

    def each_constraint(&block)
      @constraint_pairs.each(&block)
    end

    def destination_fixed_values
      EMPTY_FIXED_VALUES
    end

    private
      def initialize_keys
        @origin_key = link.match&.reference_key
        @destination_key = link.match&.target_key
        @reference_origin_key = link.reference_key
        @reference_destination_key = link.reference&.target_key
        @constraint_pairs = link.constraints.empty? ? EMPTY_KEY_PAIRS : pairs_for(link.constraints.reference_key, link.constraints.target_key)
      end

      def pairs_for(origin_key, destination_key)
        origin_key.zip(destination_key).map!(&:freeze).freeze if origin_key && destination_key
      end
  end

  class AssociationRoute::Reverse < AssociationRoute # :nodoc:
    def destination_fixed_values
      fixed_reference_values
    end

    private
      def initialize_keys
        @origin_key = link.match&.target_key
        @destination_key = link.match&.reference_key
        @reference_origin_key = link.reference&.target_key
        @reference_destination_key = link.reference_key
        @constraint_pairs = link.constraints.empty? ? EMPTY_KEY_PAIRS : pairs_for(link.constraints.target_key, link.constraints.reference_key)
      end
  end
end
