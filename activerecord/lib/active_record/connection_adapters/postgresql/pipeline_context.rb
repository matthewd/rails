# frozen_string_literal: true

require "monitor"

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      class PipelineContext # :nodoc:
        class SyncResult # :nodoc:
          def set_result(result)
            result.check
          end

          def set_error(error)
            raise error
          end

          def result
          end
        end

        def initialize(raw_connection, adapter)
          @raw_connection = raw_connection
          @adapter = adapter
          @pending_results = []
          @flushed_through = -1
          @mutex = adapter.instance_variable_get(:@lock)
          @pipeline_active = false
        end

        def pending?
          pending_result_count > 0
        end

        def pending_result_count
          @mutex.synchronize do
            @pending_results.count { |result| !result.is_a?(SyncResult) && !result.ignored }
          end
        end

        def silence_pending_results!
          @mutex.synchronize do
            @pending_results.each do |result|
              next if result.is_a?(SyncResult)

              # Note that this also affects "ignored" pending results
              result.quiet = :silent
            end
          end
        end

        def enter_pipeline_mode
          @mutex.synchronize do
            return if @pipeline_active
            pipeline_trace('PIPE_ENTER', @adapter, nil, nil, nil, :call_chain)
            @raw_connection.enter_pipeline_mode
            @pipeline_active = true
          end
        end

        def exit_pipeline_mode
          @mutex.synchronize do
            return unless @pipeline_active

            if @raw_connection.pipeline_status == PG::PQ_PIPELINE_OFF
              pipeline_trace('PIPE_GONE', @adapter, nil, nil, nil, :call_chain)
              @pipeline_active = false
              return
            end

            pipeline_trace('PIPE_EXITING', @adapter, nil, nil, nil, :call_chain)

            begin
              # Try proper cleanup - sync and collect results normally
              send_sync
              collect_remaining_results
            ensure
              # Always guarantee connection cleanup regardless of errors above

              # Drain any remaining results that weren't collected during error handling
              while result = @raw_connection.get_result
                result.clear rescue nil
              end

              @raw_connection.discard_results
              @raw_connection.exit_pipeline_mode
              @pipeline_active = false
              pipeline_trace('PIPE_EXIT', @adapter, nil, nil, nil, :call_chain)

              clear_pending_results
            end
          end
        end

        def clear_pending_results
          @mutex.synchronize do
            return if @pending_results.empty?

            results_to_clear = @pending_results.dup

            @pending_results.clear
            @flushed_through = -1
            pipeline_trace('PIPE_CLEAR', @adapter, nil, nil, nil, :call_chain)

            results_to_clear.each do |result|
              if result.is_a?(SyncResult)
                # no-op
              else
                result.set_error(ActiveRecord::StatementInvalid.new("Pipeline was cleared before query could be retrieved"))
              end
            end
          end
        end

        def pipeline_active?
          @pipeline_active
        end

        def wait_for(result)
          @mutex.synchronize do
            if index = @pending_results.index(result)
              if index >= @flushed_through
                pipeline_trace('PIPE_FLUSH', @adapter, result, result.sql) unless result.quiet
                flush_queries_through index
              else
                pipeline_trace('PIPE_WAIT', @adapter, result, result.sql) unless result.quiet
              end
              collect_results_through index
            else
              raise "Unknown result"
            end
          end

          nil
        end

        def add_query(sql, binds, type_casted_binds, prepare:, name: nil, adapter: nil, ongoing_multi_query: false, quiet: false)
          @mutex.synchronize do
            raise "Pipeline not active" unless @pipeline_active

            if @raw_connection.pipeline_status == PG::PQ_PIPELINE_OFF
              pipeline_trace('PIPE_RESTORE', @adapter, nil, nil, nil, :call_chain)
              @raw_connection.enter_pipeline_mode
              @pipeline_active = true
              clear_pending_results
            end


            # Send query to pipeline immediately
            # In pipeline mode, we must use send_query_params, not send_query
            if prepare
              # For prepared statements, we'll need to handle this differently
              # For now, fall back to send_query_params for pipeline mode
              @raw_connection.send_query_params(sql, type_casted_binds || [])
            else
              # Always use send_query_params in pipeline mode, even for queries without binds
              @raw_connection.send_query_params(sql, type_casted_binds || [])
            end

            result = ActiveRecord::PipelineResult.new(
              self,
              sql: sql,
              name: name,
              binds: binds,
              type_casted_binds: type_casted_binds,
              adapter: adapter,
              quiet: quiet,
            )

            pipeline_trace('PIPE_SEND', @adapter, result, sql, binds) unless quiet

            @pending_results << result

            unless ongoing_multi_query
              send_sync
            end

            result
          end
        end

        def add_transaction_command(sql, adapter: nil)
          add_query(sql, [], [], prepare: false, name: "TRANSACTION", adapter: adapter)
        end

        def expecting_result(name, msg = nil, quiet: false, ongoing_multi_query: false)
          @mutex.synchronize do
            raise "Pipeline not active" unless @pipeline_active

            if @raw_connection.pipeline_status == PG::PQ_PIPELINE_OFF
              pipeline_trace('PIPE_RESTORE', @adapter, nil, nil, nil, :call_chain)
              @raw_connection.enter_pipeline_mode
              @pipeline_active = true
              clear_pending_results
            end

            yield

            result = ActiveRecord::PipelineResult.new(
              self,
              sql: nil,
              name: name,
              binds: nil,
              type_casted_binds: nil,
              adapter: @adapter,
              quiet: quiet,
            )

            pipeline_trace('PIPE_EXPECT', @adapter, result, nil, nil, msg) unless quiet

            @pending_results << result

            unless ongoing_multi_query
              send_sync
            end

            result
          end
        end

        def send_sync
          @mutex.synchronize do
            @raw_connection.pipeline_sync
            @raw_connection.flush

            # sync request also implies a server flush
            @flushed_through = @pending_results.length - 1

            result = SyncResult.new
            @pending_results << result

            result
          end
        end

        def sync_all_results
          pipeline_trace('PIPE_SYNC', @adapter)
          send_sync

          # Collect all results including the sync result
          collect_remaining_results
        end

        def has_pending_results?
          @mutex.synchronize do
            @pending_results.any?
          end
        end

        private
          def get_next_result
            prev = nil
            while @raw_connection.block && (curr = @raw_connection.get_result)
              next unless curr

              prev&.clear

              #result_status_name = PG::Result.constants.grep(/^PGRES_/).find { |c| PG::Result.const_get(c) == curr.result_status }
              #$stderr.puts "get_result -> #{result_status_name}"
              # Certain result types are not followed by a nil, and so
              # must be returned immediately
              if curr.result_status == PG::PGRES_PIPELINE_SYNC # TODO: .. or COPY-related stuff
                return curr
              end

              prev = curr
            end
            #$stderr.puts "get_result -> nil"
            prev
          end

          def flush_queries_through(target_index)
            #msg = "requesting flush of results for #{@pending_results[@flushed_through + 1..target_index].reject { |r| r.is_a?(SyncResult) }.size} queries"
            #pipeline_trace('PIPE_FLUSH', @adapter, nil, nil, nil, msg)
            @raw_connection.send_flush_request
            @raw_connection.flush
            @flushed_through = @pending_results.length - 1
          end

          def collect_results_through(target_index)
            n = target_index

            while n >= 0 && pending_result = @pending_results.first
              begin
                raw_result = get_next_result
                raise "Expected result, got #{raw_result.inspect}" unless raw_result

                if (raw_result.result_status == PG::PGRES_PIPELINE_SYNC) != pending_result.is_a?(SyncResult)
                  result_status_name = PG::Result.constants.grep(/^PGRES_/).select { |c| PG::Result.const_get(c) == raw_result.result_status && !c.to_s.start_with?("PGRES_POLLING_") }
                  raise "Pipeline result mismatch: expected #{pending_result.class.name.gsub(/.*::/, "")}, got #{result_status_name.join("/")}"
                end

                #$stderr.puts "get_next_result -> #{result_status_name}"
                pending_result.set_result(raw_result)
              rescue => err
                pending_result.set_error(err)
              end

              @pending_results.shift
              @flushed_through -= 1 if @flushed_through > -1
              n -= 1
            end
          end

          def collect_remaining_results
            while pending_result = @pending_results.first
              collect_results_through(0)

              pending_result.result
            end
          end
      end
    end
  end
end
