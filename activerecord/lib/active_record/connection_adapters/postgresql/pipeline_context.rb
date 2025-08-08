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
          @mutex = Monitor.new
          @pipeline_active = false
        end

        def enter_pipeline_mode
          @mutex.synchronize do
            return if @pipeline_active
            @raw_connection.enter_pipeline_mode
            @pipeline_active = true
          end
        end

        def exit_pipeline_mode
          @mutex.synchronize do
            return unless @pipeline_active


            begin
              # Try proper cleanup - sync and collect results normally
              @raw_connection.pipeline_sync
              @raw_connection.flush
              @pending_results << SyncResult.new
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

              @pending_results.clear
              @flushed_through = -1
            end
          end
        end

        def pipeline_active?
          @pipeline_active
        end

        def wait_for(result)
          @mutex.synchronize do
            if index = @pending_results.index(result)
              flush_queries_through index
              collect_results_through index
            else
              raise "Unknown result"
            end
          end

          nil
        end

        def add_query(sql, binds, type_casted_binds, prepare:, name: nil, adapter: nil)
          @mutex.synchronize do
            raise "Pipeline not active" unless @pipeline_active


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
              adapter: adapter
            )
            
            pipeline_trace('PIPE_SEND', result.__id__, sql, binds)

            @pending_results << result

            result
          end
        end

        def add_transaction_command(sql, adapter: nil)
          # Just use add_query with no binds for transaction commands
          result = add_query(sql, [], [], prepare: false, name: "TRANSACTION", adapter: adapter)
          pipeline_trace('PIPE_TXN', result.__id__, sql)
          result
        end

        def sync_all_results
          @mutex.synchronize do
            return unless @pipeline_active

            pipeline_trace('PIPE_SYNC')
            
            # Send flush and sync, then collect all pending results
            @raw_connection.send_flush_request
            @raw_connection.flush
            @raw_connection.pipeline_sync
            @pending_results << SyncResult.new
          end

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
            while curr = @raw_connection.get_result
              # Certain result types are not followed by a nil, and so
              # must be returned immediately
              if curr.result_status == PG::PGRES_PIPELINE_SYNC # TODO: .. or COPY-related stuff
                return curr
              end

              prev = curr
            end
            prev
          end

          def flush_queries_through(target_index)
            return if target_index < @flushed_through

            pipeline_trace('PIPE_FLUSH')
            @raw_connection.send_flush_request
            @raw_connection.flush
            @flushed_through = @pending_results.length - 1
          end

          def collect_results_through(target_index)
            n = target_index

            while n >= 0 && pending_result = @pending_results.first
              begin
                raw_result = get_next_result
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
