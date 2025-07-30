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

    delegate :empty?, :to_a, :rows, :columns, :each, :first, :last, :size, :length, :count, to: :result

    def initialize(pipeline_context)
      @pipeline_context = pipeline_context
      @mutex = Monitor.new
      @result = nil
      @pending = true
      @error = nil
    end

    def then(&block)
      Promise.new(self, block)
    end

    def set_result(result)
      @mutex.synchronize do
        @result = result
        @pending = false
        
        # Immediately process the result to avoid connection state issues
        begin
          # Handle PGRES_PIPELINE_ABORTED results explicitly
          if @result.result_status == PG::PGRES_PIPELINE_ABORTED
            @error = ActiveRecord::StatementInvalid.new("Query was aborted due to an earlier error in the pipeline")
          else
            @result.check
            @final_result = @pipeline_context.instance_variable_get(:@adapter).send(:cast_result, @result)
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
        @pipeline_context.wait_for(self) if @pending
        raise @error if @error
        @final_result
      end
    end

    def pending?
      @pending
    end
  end
end
