# frozen_string_literal: true

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
      @mutex = Mutex.new
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
        return @final_result if @final_result

        begin
          @result.check
          @final_result = @pipeline_context.instance_variable_get(:@adapter).send(:cast_result, @result)
        rescue => err
          @error = err
          raise
        end
      end
    end

    def pending?
      @pending
    end
  end
end
