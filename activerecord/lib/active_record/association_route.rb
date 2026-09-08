# frozen_string_literal: true

module ActiveRecord
  # The writable reference between associated records.
  class AssociationLink # :nodoc:
    attr_reader :reference

    def initialize(reference:)
      @reference = reference
      freeze
    end

    def write_reference(reference_record, target_record)
      reference.each do |reference_column, target_column|
        value = target_record.read_attribute(target_column)
        if reference_record.read_attribute(reference_column) != value
          reference_record.write_attribute(reference_column, value)
        end
      end
    end
  end

  # A resolved association link, viewed from one reflection endpoint.
  class AssociationRoute # :nodoc:
    EMPTY_FIXED_VALUES = {}.freeze
    private_constant :EMPTY_FIXED_VALUES

    attr_reader :link, :fixed_reference_values

    def initialize(link:, reference_on:, fixed_reference_values: {})
      @link = link
      @reference_on = reference_on
      @fixed_reference_values = fixed_reference_values.freeze

      unless reference_on == :origin || reference_on == :destination
        raise ArgumentError, "Unknown association reference endpoint: #{reference_on.inspect}"
      end

      @origin_key, @destination_key, @key_pairs = orient(link.reference)
      freeze
    end

    attr_reader :origin_key, :destination_key

    def each_match(&block)
      @key_pairs.each(&block)
    end

    def values_from_origin(origin)
      @origin_key.map { |column| origin.read_attribute(column) }
    end

    def reference_on_destination?
      @reference_on == :destination
    end

    def destination_fixed_values
      reference_on_destination? ? @fixed_reference_values : EMPTY_FIXED_VALUES
    end

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
