# frozen_string_literal: true

require "monitor"

module ActiveRecord
  class PipelineResult # :nodoc:
    class Complete
      attr_reader :result
      delegate :empty?, :to_a, :rows, :columns, :each, :first, :last, :size, :length, :count, to: :result

      def initialize(result)
        @result = result
      end

      def pending?
        false
      end
    end

    def self.wrap(result)
      case result
      when self, Complete
        result
      else
        Complete.new(result)
      end
    end

    delegate :empty?, :to_a, :rows, :columns, :each, :first, :last, :size, :length, :count, to: :cast_result

    def initialize(pipeline_context, sql: nil, name: nil, binds: nil, type_casted_binds: nil, adapter: nil)
      @pipeline_context = pipeline_context
      @mutex = Monitor.new
      @result = nil
      @pending = true
      @error = nil
      @instrumentation_emitted = false
      @sql = sql
      @name = name
      @binds = binds
      @type_casted_binds = type_casted_binds
      @adapter = adapter
    end

    def then(&block)
      Promise.new(self, block)
    end

    def set_result(result)
      @mutex.synchronize do
        @result = result
        @pending = false
        
        # Check for pipeline aborted status and validate result  
        begin
          # Handle PGRES_PIPELINE_ABORTED results explicitly
          if @result.result_status == PG::PGRES_PIPELINE_ABORTED
            @error = ActiveRecord::StatementInvalid.new("Query was aborted due to an earlier error in the pipeline")
          else
            @result.check
            # Store the raw result - let normal casting flow handle type conversion
            @final_result = @result
          end
        rescue => err
          # Translate PG exceptions to ActiveRecord exceptions using the adapter's translation
          @error = @pipeline_context.instance_variable_get(:@adapter).send(:translate_exception_class, err, nil, nil)
        end
      end
    end

    def set_error(error)
      @mutex.synchronize do
        @error = error
        @pending = false
      end
    end

    def result
      @mutex.synchronize do
        # Emit instrumentation if we have context and haven't emitted yet
        if @adapter && !@instrumentation_emitted
          @adapter.log(@sql, @name, @binds, @type_casted_binds) do
            @instrumentation_emitted = true
            @pipeline_context.wait_for(self) if @pending
          end
        else
          @pipeline_context.wait_for(self) if @pending
        end
        
        raise @error if @error
        @final_result  # Return raw PG::Result for compatibility
      end
    end

    def cast_result
      # Lazily cast the result to ActiveRecord::Result when data is accessed
      @cast_result ||= begin
        raw_result = result  # Get the raw PG::Result
        adapter = @pipeline_context.instance_variable_get(:@adapter)
        adapter.send(:cast_result, raw_result)
      end
    end

    def check
      # Wait for result and raise any errors, then return consumed ActiveRecord::Result
      # This preserves pipelining while ensuring errors are raised immediately
      cast_result  # This triggers waiting, error checking, and casting
    end

    def pending?
      @pending
    end
  end
end
