# frozen_string_literal: true

require "active_support/core_ext/array/extract"

module ActiveRecord
  class Relation
    class ConditionFromHash
      attr_reader :opts

      def initialize(opts, relation)
        @opts = opts
        @relation = relation
        @invert = false
      end

      def ==(other)
        @opts == other.opts
      end

      def invert
        @invert = !@invert
        self
      end

      def references
        PredicateBuilder.references(@opts)
      end

      def relation_attributes_helper(hash, attrs, path = [])
        hash.each do |key, value|
          if value.is_a?(Hash)
            relation_attributes_helper(value, attrs, path + [key])
          else
            attrs << path + [key]
          end
        end
        attrs
      end

      def referenced_attributes
        relation_attributes_helper(@opts, [])
      end

      # TODO: consider strings vs symbols
      def except_attributes(attributes)
        new_opts = {}

        @opts.each do |key, value|
          if value.is_a?(Hash)
            new_opts[key] = except_attributes_helper(value, key, attributes)
          elsif !attributes.include?(key)
            new_opts[key] = value
          end
        end

        self.class.new(new_opts, @relation)
      end

      def except_attributes_helper(hash, parent, attributes)
        filtered_attributes = attributes.filter_map { |attr| attr.is_a?(Array) && attr.first == key && attr[1..-1] }

        new_opts = {}

        hash.each do |key, value|
          if value.is_a?(Hash)
            new_opts[key] = except_attributes_helper(value, key, filtered_attributes)
          elsif !filtered_attributes.include?([key])
            new_opts[key] = value
          end
        end

        new_opts
      end

      def predicates
        @predicate ||= evaluate
        # puts "predicates are #{@predicate}"
        @predicate
      end

      def fetch_attribute(&block)
        # TODO: this one is for sure wrong
        []
      end

      private

      def evaluate
        parts = @relation.predicate_builder.build_from_hash(opts) do |table_name|
          @relation.send(:lookup_table_klass_from_join_dependencies, table_name)
        end

        if @invert
          inverted_predicates = []
          if parts.size == 0
            parts
          elsif parts.size == 1
            only_predicate = case parts.first
            when NilClass
              raise ArgumentError, "Invalid argument for .where.not(), got nil."
            when String
              Arel::Nodes::Not.new(Arel::Nodes::SqlLiteral.new(parts.first))
            else
              parts = parts.first.invert
            end
            inverted_predicates = [ only_predicate ]
          else
            inverted_predicates = [ Arel::Nodes::Not.new(Arel::Nodes::And.new(parts)) ]
          end
          parts = inverted_predicates
        end
        parts
      end
    end

    class ConditionArelNode
      attr_reader :node

      def initialize(node)
        @node = node
      end

      def ==(other)
        @node == other.node
      end

      def ===(other)
        @node === other.node
      end

      def invert
        @node = Arel::Nodes::Not.new(@node)
      end

      def referenced_attributes
        if attr = extract_attribute(@node)
          [attr]
        elsif @node.equality? && @node.left.is_a?(Arel::Predications)
          [@node.left]
        else
          []
        end
      end

      def except_attributes(attributes, rewhere: true)
        attrs = attributes.extract! { |node|
          node.is_a?(Arel::Attribute)
        }
        non_attrs = attributes.extract! { |node|
          node.is_a?(Arel::Predications)
        }

        if !non_attrs.empty? && node.equality? && @node.left.is_a?(Arel::Predications) && non_attrs.include?(@node.left)
          nil
        else
          Arel.fetch_attribute(@node) do |attr|
            if attrs.include?(attr) || attributes.include?(attr.name.to_s)
              nil
            else
              self
            end
          end
        end
      end

      def extract_attribute(node)
        attr_node = nil
        Arel.fetch_attribute(node) do |attr|
          return if attr_node&.!= attr # all attr nodes should be the same
          attr_node = attr
        end
        attr_node
      end

      def fetch_attribute(&block)
        @node.fetch_attribute(&block)
      end

      def predicates
        @node
      end

    end

    class ConditionLiteral
      attr_reader :opts
      # TODO: combine with Arel node?
      def initialize(opts)
        @opts = opts # .reject { |o| o == "" }
        @invert = false
      end

      # These don't get called on literals
      def ==(other)
        @opts = other.opts
      end

      def referenced_attributes
        []
      end

      def except_attributes(attributes)
        self
      end

      def fetch_attribute(&block)
        []
      end

      def invert
        @invert = !@invert
        self
      end

      def predicates
        pred = case @opts
        when String
          Arel.sql(@opts)
        when Array
          @opts.map do |node|
            if ::String === node
              node = Arel.sql(node)
            end
            Arel::Nodes::Grouping.new(node)
          end
        else
          node
        end

        if @invert
          Arel::Nodes::Not.new(pred)
        else
          pred
        end
      end
    end

    class WhereClause # :nodoc:
      delegate :any?, :empty?, to: :predicates

      def initialize(predicates)
        @predicates = predicates
      end

      def realize!
        preds = []
        if !self.frozen? && !@predicates.nil?
          preds = @predicates.map do |p|
            if p.respond_to?(:predicates)
              p.predicates
            else
              p
            end
          end.flatten
        end
        @realized_predicates = preds
      end

      def +(other)
        WhereClause.new(predicates + other.predicates)
      end

      def -(other)
        WhereClause.new(predicates - other.predicates)
      end

      def |(other)
        WhereClause.new(predicates | other.predicates)
      end

      def extract_attributes
        predicates.flat_map(&:referenced_attributes)
      end

      def merge(other, rewhere = nil)
        predicates =
          if rewhere
            attributes = other.extract_attributes
            self.predicates.filter_map { |p| p.except_attributes(attributes) }
          else
            predicates_unreferenced_by(other)
            # TODO next:
            #attributes = other.predicates.flat_map(&:referenced_attributes)
            #self.predicates.filter_map { |p| p.except_attributes(attributes, rewhere: false) }
          end

        WhereClause.new(predicates | other.predicates)
      end

      def except(*columns)
        WhereClause.new(predicates.filter_map { |p| p.except_attributes(columns) })
      end

      def or(other)
        left = self - other
        common = self - left
        right = other - common

        if left.empty? || right.empty?
          common
        else
          left = left.ast
          left = left.expr if left.is_a?(Arel::Nodes::Grouping)

          right = right.ast
          right = right.expr if right.is_a?(Arel::Nodes::Grouping)

          or_clause = Arel::Nodes::Or.new(left, right)

          common.predicates << Arel::Nodes::Grouping.new(or_clause)
          common
        end
      end

      def to_h(table_name = nil, equality_only: false)
        realize!

        equalities(@realized_predicates, equality_only).each_with_object({}) do |node, hash|
          next if table_name&.!= node.left.relation.name
          name = node.left.name.to_s
          value = extract_node_value(node.right)
          hash[name] = value
        end
      end

      def ast
        predicates = predicates_with_wrapped_sql_literals

        predicates.one? ? predicates.first : Arel::Nodes::And.new(predicates)
      end

      def ==(other)
        other.is_a?(WhereClause) &&
          predicates == other.predicates
      end
      alias :eql? :==

      def hash
        [self.class, predicates].hash
      end

      def invert
        if predicates.size == 1
          inverted_predicates = [ invert_predicate(predicates.first) ]
        else
          inverted_predicates = [ Arel::Nodes::Not.new(ast) ]
        end

        WhereClause.new(inverted_predicates)
      end

      def self.empty
        @empty ||= new([])
      end

      def contradiction?
        realize!

        @realized_predicates.any? do |x|
          case x
          when Arel::Nodes::In
            Array === x.right && x.right.empty?
          when Arel::Nodes::Equality
            x.right.respond_to?(:unboundable?) && x.right.unboundable?
          end
        end
      end

      protected
        attr_reader :predicates

        def referenced_columns
          hash = {}
          each_attributes { |attr, node| hash[attr] = node }
          hash
        end

      private
        def each_attributes
          realize!
          @realized_predicates.each do |node|
            attr = extract_attribute(node) || begin
              node.left if equality_node?(node) && node.left.is_a?(Arel::Predications)
            end

            yield attr, node if attr
          end
        end

        def extract_attribute(node)
          attr_node = nil
          Arel.fetch_attribute(node) do |attr|
            return if attr_node&.!= attr # all attr nodes should be the same
            attr_node = attr
          end
          attr_node
        end

        def equalities(predicates, equality_only)
          equalities = []

          predicates.each do |node|
            if equality_only ? Arel::Nodes::Equality === node : equality_node?(node)
              equalities << node
            elsif node.is_a?(Arel::Nodes::And)
              equalities.concat equalities(node.children, equality_only)
            end
          end

          equalities
        end

        def predicates_unreferenced_by(other)
          realize!

          referenced_columns = other.referenced_columns

          @realized_predicates.reject do |node|
            attr = extract_attribute(node) || begin
              node.left if equality_node?(node) && node.left.is_a?(Arel::Predications)
            end

            attr && referenced_columns[attr]
          end
        end

        def equality_node?(node)
          !node.is_a?(String) && node.equality?
        end

        def invert_predicate(node)
          case node
          when NilClass
            raise ArgumentError, "Invalid argument for .where.not(), got nil."
          when String
            Arel::Nodes::Not.new(Arel::Nodes::SqlLiteral.new(node))
          else

            node.invert
          end
        end

        def except_predicates(columns)
          attrs = columns.extract! { |node|
            node.is_a?(Arel::Attribute)
          }
          non_attrs = columns.extract! { |node|
            node.is_a?(Arel::Predications)
          }

          realize!

          @realized_predicates.reject do |node|
            if !non_attrs.empty? && node.equality? && node.left.is_a?(Arel::Predications)
              non_attrs.include?(node.left)
            end || Arel.fetch_attribute(node) do |attr|
              attrs.include?(attr) || columns.include?(attr.name.to_s)
            end
          end
        end

        def predicates_with_wrapped_sql_literals
          non_empty_predicates.map do |node|
            case node
            when Arel::Nodes::SqlLiteral, ::String
              wrap_sql_literal(node)
            else node
            end
          end
        end

        ARRAY_WITH_EMPTY_STRING = [""]
        def non_empty_predicates
          realize!
          @realized_predicates - ARRAY_WITH_EMPTY_STRING
        end

        # TODO: this might not be unneeded anymore
        def wrap_sql_literal(node)
          if ::String === node
            node = Arel.sql(node)
          end
          Arel::Nodes::Grouping.new(node)
        end

        def extract_node_value(node)
          if node.respond_to?(:value_before_type_cast)
            node.value_before_type_cast
          elsif Array === node
            node.map { |v| extract_node_value(v) }
          end
        end
    end
  end
end
