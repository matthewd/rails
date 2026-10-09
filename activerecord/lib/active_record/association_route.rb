# :markup: markdown
# frozen_string_literal: true

require "active_support/weak_key_map"

module ActiveRecord
  # An association's writable foreign-key reference and query-only constraints,
  # oriented from the record holding the foreign key to the referenced record.
  #
  # Keys come in two forms. Declared keys keep the names given to the
  # association, for APIs that resolve attribute aliases themselves
  # (+read_attribute+, +write_attribute+, +where+, Arel). Physical keys hold
  # those names resolved through the aliases of the classes the link was built
  # for, for APIs that don't (+_read_attribute+, +_write_attribute+,
  # +_has_attribute?+, and dirty tracking). Passing a physical name to a
  # resolving API resolves it twice.
  #
  # Physical keys apply to records of the classes the link was built for, or of
  # any classes whose link compares equal. Links don't retain those classes.
  class AssociationLink # :nodoc:
    EMPTY_FIXED_VALUES = {}.freeze
    NO_ALIASES = {}.freeze
    private_constant :EMPTY_FIXED_VALUES, :NO_ALIASES

    # Builds the link, choosing DeprecatedIdWriter when writing its reference
    # would rely on +write_attribute+'s handling of "id".
    def self.for(reference_class: nil, **options)
      link = new(reference_class: reference_class, **options)
      if link.send(:writes_mapped_id?, reference_class)
        DeprecatedIdWriter.new(reference_class: reference_class, **options)
      else
        link
      end
    end

    attr_reader :reference, :reference_key, :constraints, :match

    # Attributes written alongside the foreign key, such as a polymorphic type.
    attr_reader :fixed_values

    # Physical counterparts of the keys and fixed values above.
    attr_reader :_reference_key, :_target_key, :_match_reference_key, :_match_target_key, :_fixed_values

    def initialize(reference: nil, reference_key: reference&.reference_key, constraints: Key::Mapping.empty, fixed_values: EMPTY_FIXED_VALUES, reference_class: nil, target_class: nil)
      @reference = reference
      @reference_key = reference_key
      @constraints = constraints
      @fixed_values = fixed_values.freeze
      @match = (constraints + reference).normalize if reference

      reference_aliases = reference_class ? reference_class.attribute_aliases : NO_ALIASES
      target_aliases = target_class ? target_class.attribute_aliases : NO_ALIASES
      @_reference_key = reference_key&.resolve_aliases(reference_aliases)
      @_target_key = reference&.target_key&.resolve_aliases(target_aliases)
      @_match_reference_key = @match&.reference_key&.resolve_aliases(reference_aliases)
      @_match_target_key = @match&.target_key&.resolve_aliases(target_aliases)
      @_fixed_values = resolve_fixed_values(@fixed_values, reference_aliases)

      @hash = [self.class, reference, reference_key, constraints, @fixed_values,
        @_reference_key, @_target_key, @_match_reference_key, @_match_target_key].hash
      freeze
    end

    def ==(other)
      other.instance_of?(self.class) &&
        reference == other.reference &&
        reference_key == other.reference_key &&
        constraints == other.constraints &&
        fixed_values == other.fixed_values &&
        _reference_key == other._reference_key &&
        _target_key == other._target_key &&
        _match_reference_key == other._match_reference_key &&
        _match_target_key == other._match_target_key
    end
    alias_method :eql?, :==

    attr_reader :hash

    def reference_values(target_record)
      each_reference_value(target_record).to_h
    end

    # The records must be of the link's classes; see the class comment.
    def reference_needs_update?(reference_record, target_record)
      if _reference_key.all? { |column| reference_record._has_attribute?(column) }
        target_columns = _target_key.columns if target_record
        _reference_key.each_with_index do |column, index|
          return true if reference_record._read_attribute(column) != target_record&._read_attribute(target_columns[index])
        end
      end

      _fixed_values.any? do |column, value|
        reference_record._has_attribute?(column) && reference_record._read_attribute(column) != value
      end
    end

    # The records must be of the link's classes; see the class comment.
    def write_reference(reference_record, target_record, force: true)
      target_columns = _target_key.columns if target_record
      _reference_key.each_with_index do |column, index|
        value = target_record&._read_attribute(target_columns[index])
        next if !force && reference_record._read_attribute(column) == value
        reference_record._write_attribute(column, value)
      end

      _fixed_values.each do |column, value|
        next if !force && reference_record._read_attribute(column) == value
        reference_record._write_attribute(column, value)
      end unless _fixed_values.empty?
    end

    # Clears the reference on +reference_record+, keeping any columns in
    # +preserve+. Fixed-value columns are cleared too unless +fixed_values+ is
    # false; without +force+, only those not already nil.
    def clear_reference(reference_record, preserve: nil, fixed_values: true, force: true)
      _reference_key.each do |column|
        next if preserve&.include?(column)
        reference_record._write_attribute(column, nil)
      end
      return unless fixed_values

      _fixed_values.each_key do |column|
        next if !force && reference_record._read_attribute(column).nil?
        reference_record._write_attribute(column, nil)
      end
    end

    private
      # +write_attribute+ still writes "id" to a different primary key, which
      # is deprecated; _write_attribute would write the "id" attribute itself.
      def writes_mapped_id?(reference_class)
        return false unless reference_class

        primary_key = reference_class.primary_key
        primary_key && primary_key != "id" && !reference_class.composite_primary_key? &&
          (_reference_key.include?("id") || _fixed_values.key?("id"))
      end

      def resolve_fixed_values(fixed_values, aliases)
        return fixed_values if fixed_values.empty? || fixed_values.none? { |column, _| aliases.key?(column) }
        fixed_values.to_h { |column, value| [-(aliases[column] || column), value] }.freeze
      end

      def each_reference_value(target_record, &)
        return enum_for(__method__, target_record) unless block_given?

        each_reference_key_value(target_record, &)
        fixed_values.each(&)
      end

      # Yields declared reference columns with the values to assign them.
      def each_reference_key_value(target_record)
        return enum_for(__method__, target_record) unless block_given?

        target_columns = reference.target_key.columns if target_record

        reference_key.each_with_index do |column, index|
          yield column, target_record&.read_attribute(target_columns[index])
        end
      end
  end

  # Writes through +write_attribute+ by declared name, keeping its deprecated
  # mapping of "id" to a model's different primary key until that is removed.
  # Reads and comparisons still treat "id" as the attribute itself.
  class AssociationLink::DeprecatedIdWriter < AssociationLink # :nodoc:
    def write_reference(reference_record, target_record, force: true)
      each_reference_value(target_record) do |column, value|
        next if !force && reference_record.read_attribute(column) == value
        reference_record.write_attribute(column, value)
      end
    end

    def clear_reference(reference_record, preserve: nil, fixed_values: true, force: true)
      reference_key.zip(_reference_key) do |column, physical_column|
        next if preserve&.include?(physical_column)
        reference_record.write_attribute(column, nil)
      end
      return unless fixed_values

      self.fixed_values.each_key.zip(_fixed_values.each_key) do |column, physical_column|
        next if !force && reference_record._read_attribute(physical_column).nil?
        reference_record.write_attribute(column, nil)
      end
    end
  end

  # A link from an owner (origin) to its associated record (destination), with
  # declared and physical keys as described for AssociationLink.
  class AssociationRoute # :nodoc:
    EMPTY_FIXED_VALUES = {}.freeze
    EMPTY_KEY_PAIRS = [].freeze
    STRING_NORMALIZER = ActiveSupport::Ractors.shareable_lambda { |value| value&.to_s }.freeze
    private_constant :EMPTY_FIXED_VALUES, :EMPTY_KEY_PAIRS, :STRING_NORMALIZER

    class Cache < ActiveSupport::WeakKeyMap # :nodoc:
      NIL_KEY = Object.new.freeze
      private_constant :NIL_KEY

      def self.for(key)
        registry = ActiveSupport::Ractors.store_if_absent(:active_record_association_route_caches) { new }
        registry.fetch_or_store(key) { new }
      end

      def self.clear(key)
        ActiveSupport::Ractors[:active_record_association_route_caches]&.delete(key)
      end

      def fetch_or_store(key)
        super(key.nil? ? NIL_KEY : key)
      end
    end

    attr_reader :link

    def initialize(link:)
      @link = link
      initialize_keys
      @key_pairs = pairs_for(origin_key, destination_key)
      @reference_pairs = if link.constraints.empty?
        @key_pairs
      else
        pairs_for(reference_origin_key, reference_destination_key)
      end
      @hash = [self.class, link].hash
      freeze
    end

    def ==(other)
      other.class == self.class && link == other.link
    end
    alias_method :eql?, :==

    attr_reader :hash

    attr_reader :origin_key, :destination_key, :reference_origin_key, :reference_destination_key

    # Physical counterparts of the keys above. Like the link's, they apply to
    # records of the classes the route was resolved for, or of any classes
    # whose route compares equal.
    attr_reader :_origin_key, :_destination_key, :_reference_origin_key, :_reference_destination_key

    def predicate_accepts_bare_id?
      !origin_key.composite? && !reference_origin_key.composite?
    end

    def each_match(&block)
      @key_pairs.each(&block)
    end

    # Per-column conversions for comparing origin and destination key values.
    # A nil list or entry leaves the corresponding values unchanged.
    def match_normalizers(origin_class:, destination_class:)
      # Schema contexts are replaced on reload, so key by them to avoid
      # reusing conversions computed from outdated attribute types.
      cache = Cache.for(self)
      by_destination = cache.fetch_or_store(origin_class.schema_context) { Cache.new }
      by_destination.fetch_or_store(destination_class.schema_context) do
        build_match_normalizers(origin_class, destination_class)
      end
    end

    # The records must be of classes this route applies to.
    def key_values_match?(origin, destination)
      origin_normalizers, destination_normalizers = match_normalizers(origin_class: origin.class, destination_class: destination.class)
      _origin_key._value_of(origin, origin_normalizers) == _destination_key._value_of(destination, destination_normalizers)
    end

    def each_matching_origin(origins, destination)
      return enum_for(__method__, origins, destination) unless block_given?

      # The destination's conversion depends on the owner's attribute types, so
      # cache its converted value separately for each owner class.
      comparisons_by_class = {}
      origins.each do |origin|
        origin_normalizers, destination_value = comparisons_by_class[origin.class] ||= begin
          origin_normalizers, destination_normalizers = match_normalizers(origin_class: origin.class, destination_class: destination.class)
          [origin_normalizers, destination_key.value_of(destination, destination_normalizers)]
        end

        yield origin if origin_key.value_of(origin, origin_normalizers) == destination_value
      end
    end

    # +origin+ must be of a class this route applies to.
    def origin_reference_complete?(origin)
      _reference_origin_key.all? { |column| !origin._read_attribute(column).nil? }
    end

    def reference_needs_update?(origin, destination)
      @link.reference_needs_update?(origin, destination)
    end

    def each_reference(&block)
      @reference_pairs.each(&block)
    end

    def each_constraint(&block)
      @constraint_pairs.each(&block)
    end

    def origin_fixed_values
      @link.fixed_values
    end

    def destination_fixed_values
      EMPTY_FIXED_VALUES
    end

    # The records must be of classes this route applies to.
    def write(origin, destination, force: true)
      @link.write_reference(origin, destination, force: force)
    end

    # Clears the reference held by +reference_record+, which must be of the
    # class this route resolved for that side; see AssociationLink#clear_reference.
    def clear_reference(reference_record, **)
      @link.clear_reference(reference_record, **)
    end

    private
      def initialize_keys
        @origin_key = link.match&.reference_key
        @destination_key = link.match&.target_key
        @reference_origin_key = link.reference_key
        @reference_destination_key = link.reference&.target_key
        @_origin_key = link._match_reference_key
        @_destination_key = link._match_target_key
        @_reference_origin_key = link._reference_key
        @_reference_destination_key = link._target_key
        @constraint_pairs = link.constraints.empty? ? EMPTY_KEY_PAIRS : pairs_for(link.constraints.reference_key, link.constraints.target_key)
      end

      def build_match_normalizers(origin_class, destination_class)
        origin_normalizers = destination_normalizers = nil
        each_match.with_index do |(origin_column, destination_column), index|
          origin_type = origin_class.type_for_attribute(origin_column)
          destination_type = destination_class.type_for_attribute(destination_column)
          origin_normalizer, destination_normalizer = normalizers_for_types(origin_type, destination_type)

          if origin_normalizer
            (origin_normalizers ||= Array.new(@key_pairs.length))[index] = origin_normalizer
          end
          if destination_normalizer
            (destination_normalizers ||= Array.new(@key_pairs.length))[index] = destination_normalizer
          end
        end
        [origin_normalizers&.freeze, destination_normalizers&.freeze].freeze
      end

      def normalizers_for_types(origin_type, destination_type)
        unless origin_type.type == destination_type.type
          [STRING_NORMALIZER, STRING_NORMALIZER]
        end
      end

      def pairs_for(origin_key, destination_key)
        origin_key.zip(destination_key).map!(&:freeze).freeze if origin_key && destination_key
      end
  end

  class AssociationRoute::Reverse < AssociationRoute # :nodoc:
    def reference_needs_update?(origin, destination)
      @link.reference_needs_update?(destination, origin)
    end

    def origin_fixed_values
      EMPTY_FIXED_VALUES
    end

    def destination_fixed_values
      @link.fixed_values
    end

    def write(origin, destination, force: true)
      @link.write_reference(destination, origin, force: force)
    end

    # The attributes #write would assign to a destination, for building one.
    def destination_attributes(origin)
      @link.reference_values(origin)
    end

    private
      def initialize_keys
        @origin_key = link.match&.target_key
        @destination_key = link.match&.reference_key
        @reference_origin_key = link.reference&.target_key
        @reference_destination_key = link.reference_key
        @_origin_key = link._match_target_key
        @_destination_key = link._match_reference_key
        @_reference_origin_key = link._target_key
        @_reference_destination_key = link._reference_key
        @constraint_pairs = link.constraints.empty? ? EMPTY_KEY_PAIRS : pairs_for(link.constraints.target_key, link.constraints.reference_key)
      end
  end
end
