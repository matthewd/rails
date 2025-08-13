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

      def check
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
    attr_reader :sql, :ignored
    attr_accessor :quiet

    def initialize(pipeline_context, sql: nil, name: nil, binds: nil, type_casted_binds: nil, log_kwargs: nil, adapter: nil, quiet: false, stmt_key: nil, trace_action: nil)
      @pipeline_context = pipeline_context
      @mutex = Monitor.new
      @condition = @mutex.new_cond
      @result = nil
      @pending = true
      @error = nil
      @instrumentation_emitted = false
      @sql = sql
      @name = name
      @binds = binds
      @type_casted_binds = type_casted_binds
      @log_kwargs = log_kwargs
      @adapter = adapter
      @quiet = quiet
      @stmt_key = stmt_key
      @trace_action = trace_action
    end

    def then(&block)
      Promise.new(self, block)
    end

    def set_result(raw_result)
      @mutex.synchronize do
        result_status_name = PG::Result.constants.grep(/^PGRES_/).find { |c| PG::Result.const_get(c) == raw_result.result_status && !c.start_with?("PGRES_POLLING_") }&.to_s

        @result = raw_result
        @pending = false
        @condition.signal

        # Check for pipeline aborted status and validate result
        begin
          # Handle PGRES_PIPELINE_ABORTED results explicitly
          if @result.result_status == PG::PGRES_PIPELINE_ABORTED
            @error = ActiveRecord::StatementInvalid.new("Query was aborted due to an earlier error in the pipeline")
            pipeline_trace('PIPE_ABORT', @adapter, self, @sql, nil, result_status_name)
          else
            if @ignored && @quiet != :no_log
              # No-one else to instrument it, so we'll do it here
              # FIXME: I've fixed the other callsite... is it still bad
              # for us to emit instrumentation while holding the mutex
              # here?
              emit_instrumentation do
                @result.check
              end
            else
              @result.check
            end

            # Store the raw result - let normal casting flow handle type conversion
            @final_result = @result
            keyword =
              if @quiet == :silent
                nil
              elsif @trace_action
                @trace_action
              elsif @ignored
                'PIPE_ASSUMED'
              elsif @quiet && @stmt_key
                'PIPE_EXECUTE'
              elsif @quiet
                'PIPE_QUERY'
              else
                'PIPE_RECV'
              end

            status_text = result_status_name || "OK"
            status_text += " (#{@result.cmd_tuples} rows)" if @result.result_status == PG::PGRES_TUPLES_OK

            status_text = "#{@stmt_key} → #{status_text}" if @stmt_key

            pipeline_trace(keyword, @adapter, self, @sql, @binds, status_text) if keyword
            #$stderr.puts @result.to_a.inspect

            @adapter.send(:verified!)
          end
        rescue => err
          # Translate PG exceptions to ActiveRecord exceptions using the adapter's translation
          translated_exception = @pipeline_context.instance_variable_get(:@adapter).send(:translate_exception_class, err, nil, nil)
          begin
            raise translated_exception
          rescue => reraised_exception
            @error = reraised_exception
            pipeline_trace('PIPE_ERROR', @adapter, self, @sql, nil, "#{result_status_name} → #{@error.message}")
          end
        end
      end
    end

    def assume_success
      @mutex.synchronize do
        @ignored = true
      end
      self
    end

    def set_error(error)
      @mutex.synchronize do
        @error = error
        @pending = false
        @condition.signal
        pipeline_trace('PIPE_ERROR', @adapter, self, @sql, nil, error.message)
      end
    end

    # Emit instrumentation if we have context and haven't emitted yet
    def emit_instrumentation
      should_emit = false

      if !@instrumentation_emitted
        @mutex.synchronize do
          should_emit = @adapter && !@instrumentation_emitted
          @instrumentation_emitted = true
        end
      end

      if should_emit
        @adapter.send(:log, @sql, @name, @binds, @type_casted_binds, **@log_kwargs) do |notification_payload|
          yield if block_given?

          @mutex.synchronize do
            if @result
              notification_payload[:affected_rows] = @result.cmd_tuples
              notification_payload[:row_count] = @result.ntuples
            end
          end
        end
      else
        yield if block_given?
      end
    end

    def result
      raise "Can't consume ignored result" if @ignored

      emit_instrumentation do
        should_wait = false

        @mutex.synchronize do
          if @pending
            should_wait = true
          end
        end

        if should_wait
          @pipeline_context.wait_for(self)
        end
      end

      raise @error if @error
      @final_result  # Return raw PG::Result for compatibility
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
