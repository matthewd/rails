# frozen_string_literal: true

# :markup: markdown
module Arel # :nodoc: all
  module Collectors
    class Bind
      attr_accessor :retryable

      def initialize
        @binds = []
      end

      def <<(str)
        self
      end

      def add_bind(bind, &)
        @binds << bind
        self
      end

      def add_binds(binds, proc_for_binds = nil, &)
        @binds.concat proc_for_binds ? binds.map(&proc_for_binds) : binds
        self
      end

      def add_bind_rows(columns, procs_for_binds, &)
        width = columns.size
        first = @binds.size
        columns.each_with_index do |column, index|
          proc_for_binds = procs_for_binds[index]
          position = first + index
          column.each do |value|
            @binds[position] = proc_for_binds.call(value)
            position += width
          end
        end
        self
      end

      def value
        @binds
      end
    end
  end
end
