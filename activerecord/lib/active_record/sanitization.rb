# frozen_string_literal: true

module ActiveRecord
  module Sanitization
    extend ActiveSupport::Concern

    module ClassMethods
      # Accepts an array of SQL conditions and sanitizes them into a valid
      # SQL fragment for a WHERE clause.
      #
      #   sanitize_sql_for_conditions(["name=? and group_id=?", "foo'bar", 4])
      #   # => "name='foo''bar' and group_id=4"
      #
      #   sanitize_sql_for_conditions(["name=:name and group_id=:group_id", name: "foo'bar", group_id: 4])
      #   # => "name='foo''bar' and group_id='4'"
      #
      #   sanitize_sql_for_conditions(["name='%s' and group_id='%s'", "foo'bar", 4])
      #   # => "name='foo''bar' and group_id='4'"
      #
      # This method will NOT sanitize an SQL string since it won't contain
      # any conditions in it and will return the string as is.
      #
      #   sanitize_sql_for_conditions("name='foo''bar' and group_id='4'")
      #   # => "name='foo''bar' and group_id='4'"
      #
      # Note that this sanitization method is not schema-aware, hence won't do any type casting
      # and will directly use the database adapter's +quote+ method.
      # For MySQL specifically this means that numeric parameters will be quoted as strings
      # to prevent query manipulation attacks.
      #
      #   sanitize_sql_for_conditions(["role = ?", 0])
      #   # => "role = '0'"
      def sanitize_sql_for_conditions(condition)
        return nil if condition.blank?

        case condition
        when Array; sanitize_sql_array(condition)
        else        condition
        end
      end
      alias :sanitize_sql :sanitize_sql_for_conditions

      def assemble_sql_for_conditions(condition)
        return nil if condition.blank?
        return condition if Arel.arel_node?(condition)

        case condition
        when Array;  compile_sql_array(condition)
        when String; Arel.sql(condition)
        else         condition
        end
      end

      # Accepts an array or hash of SQL conditions and sanitizes them into
      # a valid SQL fragment for a SET clause.
      #
      #   sanitize_sql_for_assignment(["name=? and group_id=?", nil, 4])
      #   # => "name=NULL and group_id=4"
      #
      #   sanitize_sql_for_assignment(["name=:name and group_id=:group_id", name: nil, group_id: 4])
      #   # => "name=NULL and group_id=4"
      #
      #   Post.sanitize_sql_for_assignment({ name: nil, group_id: 4 })
      #   # => "`posts`.`name` = NULL, `posts`.`group_id` = 4"
      #
      # This method will NOT sanitize an SQL string since it won't contain
      # any conditions in it and will return the string as is.
      #
      #   sanitize_sql_for_assignment("name=NULL and group_id='4'")
      #   # => "name=NULL and group_id='4'"
      #
      # Note that this sanitization method is not schema-aware, hence won't do any type casting
      # and will directly use the database adapter's +quote+ method.
      # For MySQL specifically this means that numeric parameters will be quoted as strings
      # to prevent query manipulation attacks.
      #
      #   sanitize_sql_for_assignment(["role = ?", 0])
      #   # => "role = '0'"
      def sanitize_sql_for_assignment(assignments, default_table_name = table_name)
        case assignments
        when Array; sanitize_sql_array(assignments)
        when Hash;  sanitize_sql_hash_for_assignment(assignments, default_table_name)
        else        assignments
        end
      end

      def assemble_sql_for_assignment(assignments, default_table_name = table_name)
        case assignments
        when Array; compile_sql_array(assignments)
        when Hash;  assemble_sql_hash_for_assignment(assignments, default_table_name)
        else        assignments
        end
      end

      # Accepts an array, or string of SQL conditions and sanitizes
      # them into a valid SQL fragment for an ORDER clause.
      #
      #   sanitize_sql_for_order([Arel.sql("field(id, ?)"), [1,3,2]])
      #   # => "field(id, 1,3,2)"
      #
      #   sanitize_sql_for_order("id ASC")
      #   # => "id ASC"
      def sanitize_sql_for_order(condition)
        if condition.is_a?(Array) && condition.first.to_s.include?("?")
          disallow_raw_sql!(
            [condition.first],
            permit: connection.column_name_with_order_matcher
          )

          # Ensure we aren't dealing with a subclass of String that might
          # override methods we use (e.g. Arel::Nodes::SqlLiteral).
          if condition.first.kind_of?(String) && !condition.first.instance_of?(String)
            condition = [String.new(condition.first), *condition[1..-1]]
          end

          Arel.sql(sanitize_sql_array(condition))
        else
          condition
        end
      end

      def assemble_sql_for_order(condition)
        if condition.is_a?(Array) && condition.first.to_s.include?("?")
          disallow_raw_sql!(
            [condition.first],
            permit: connection.column_name_with_order_matcher
          )

          # Ensure we aren't dealing with a subclass of String that might
          # override methods we use (e.g. Arel::Nodes::SqlLiteral).
          if condition.first.kind_of?(String) && !condition.first.instance_of?(String)
            condition = [String.new(condition.first), *condition[1..-1]]
          end

          compile_sql_array(condition)
        else
          condition
        end
      end

      # Sanitizes a hash of attribute/value pairs into SQL conditions for a SET clause.
      #
      #   sanitize_sql_hash_for_assignment({ status: nil, group_id: 1 }, "posts")
      #   # => "`posts`.`status` = NULL, `posts`.`group_id` = 1"
      def sanitize_sql_hash_for_assignment(attrs, table)
        c = connection
        attrs.map do |attr, value|
          type = type_for_attribute(attr)
          value = type.serialize(type.cast(value))
          "#{c.quote_table_name_for_assignment(table, attr)} = #{c.quote(value)}"
        end.join(", ")
      end

      def assemble_sql_hash_for_assignment(attrs, table)
        c = connection
        values = []

        sql = attrs.map do |attr, value|
          type = type_for_attribute(attr)
          values << type.serialize(type.cast(value))

          "#{c.quote_table_name_for_assignment(table, attr)} = ?"
        end.join(", ")

        Arel.sql(sql, *values)
      end

      # Sanitizes a +string+ so that it is safe to use within an SQL
      # LIKE statement. This method uses +escape_character+ to escape all
      # occurrences of itself, "_" and "%".
      #
      #   sanitize_sql_like("100% true!")
      #   # => "100\\% true!"
      #
      #   sanitize_sql_like("snake_cased_string")
      #   # => "snake\\_cased\\_string"
      #
      #   sanitize_sql_like("100% true!", "!")
      #   # => "100!% true!!"
      #
      #   sanitize_sql_like("snake_cased_string", "!")
      #   # => "snake!_cased!_string"
      def sanitize_sql_like(string, escape_character = "\\")
        if string.include?(escape_character) && escape_character != "%" && escape_character != "_"
          string = string.gsub(escape_character, '\0\0')
        end

        string.gsub(/(?=[%_])/, escape_character)
      end

      # Accepts an array of conditions. The array has each value
      # sanitized and interpolated into the SQL statement. If using named bind
      # variables in SQL statements where a colon is required verbatim use a
      # backslash to escape.
      #
      #   sanitize_sql_array(["name=? and group_id=?", "foo'bar", 4])
      #   # => "name='foo''bar' and group_id=4"
      #
      #   sanitize_sql_array(["name=:name and group_id=:group_id", name: "foo'bar", group_id: 4])
      #   # => "name='foo''bar' and group_id=4"
      #
      #   sanitize_sql_array(["TO_TIMESTAMP(:date, 'YYYY/MM/DD HH12\\:MI\\:SS')", date: "foo"])
      #   # => "TO_TIMESTAMP('foo', 'YYYY/MM/DD HH12:MI:SS')"
      #
      #   sanitize_sql_array(["name='%s' and group_id='%s'", "foo'bar", 4])
      #   # => "name='foo''bar' and group_id='4'"
      #
      # Note that this sanitization method is not schema-aware, hence won't do any type casting
      # and will directly use the database adapter's +quote+ method.
      # For MySQL specifically this means that numeric parameters will be quoted as strings
      # to prevent query manipulation attacks.
      #
      #   sanitize_sql_array(["role = ?", 0])
      #   # => "role = '0'"
      def sanitize_sql_array(ary)
        connection.to_sql(compile_sql_array(ary))
      end

      def compile_sql_array(ary)
        statement, *values = ary
        if values.first.is_a?(Hash) && /:\w+/.match?(statement)
          bind_vars = values.first
          Arel.sql(statement, **bind_vars.transform_values { |v| prepare_bind_value(v) })
        elsif statement.include?("?")
          Arel.sql(statement, *values.map { |v| prepare_bind_value(v) })
        elsif statement.blank?
          statement
        else
          statement % values.collect { |value| connection.quote_string(value.to_s) }
        end
      end

      def disallow_raw_sql!(args, permit: connection.column_name_matcher) # :nodoc:
        unexpected = nil
        args.each do |arg|
          next if arg.is_a?(Symbol) || Arel.arel_node?(arg) || permit.match?(arg.to_s.strip)
          (unexpected ||= []) << arg
        end

        if unexpected
          raise(ActiveRecord::UnknownAttributeReference,
            "Dangerous query method (method whose arguments are used as raw " \
            "SQL) called with non-attribute argument(s): " \
            "#{unexpected.map(&:inspect).join(", ")}." \
            "This method should not be called with user-provided values, such as request " \
            "parameters or model attributes. Known-safe values can be passed " \
            "by wrapping them in Arel.sql()."
          )
        end
      end

      private
        def prepare_bind_value(value, c = connection)
          if ActiveRecord::Relation === value
            if value.eager_loading?
              value.send(:apply_join_dependency) do |relation, join_dependency|
                relation = join_dependency.apply_column_aliases(relation)
                relation.arel.ast
              end
            else
              value.arel.ast
            end
          else
            if value.respond_to?(:map) && !value.acts_like?(:string)
              values = value.map { |v| v.respond_to?(:id_for_database) ? v.id_for_database : v }
              if values.empty?
                c.cast_bound_value(nil)
              else
                values.map! { |v| c.cast_bound_value(v) }
              end
            else
              value = value.id_for_database if value.respond_to?(:id_for_database)
              c.cast_bound_value(value)
            end
          end
        end
    end
  end
end
