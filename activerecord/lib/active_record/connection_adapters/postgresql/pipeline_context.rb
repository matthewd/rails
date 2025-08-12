# frozen_string_literal: true

require "monitor"

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      class PipelineContext # :nodoc:
        TRACK_SYNCS = true

        class SyncResult # :nodoc:
          def set_result(result)
            result.check
          end

          def set_error(error)
            raise error
          end

          def result
          end

          def ignored
            false
          end
        end

        def initialize(adapter)
          @adapter = adapter
          @pending_results = []
          @in_flight_results = {}
          @flushed_through = -1
          @pipeline_active = false
        end

        def raw_connection
          @adapter.instance_variable_get(:@raw_connection)
        end

        def synchronize(&block)
          @adapter.instance_variable_get(:@lock).synchronize(&block)
        end

        def pending?
          pending_result_count > 0
        end

        def pending_result_count
          synchronize do
            @pending_results.count { |result| !result.is_a?(SyncResult) && !result.ignored }
          end
        end

        def silence_pending_results!
          synchronize do
            @pending_results.each do |result|
              next if result.is_a?(SyncResult)

              # Note that this also affects "ignored" pending results
              result.quiet = :silent
            end
          end
        end

        def enter_pipeline_mode
          synchronize do
            return if @pipeline_active
            pipeline_trace('PIPE_ENTER', @adapter, nil, nil, nil, :call_chain)
            raw_connection.enter_pipeline_mode
            @pipeline_active = true
          end
        end

        def exit_pipeline_mode
          synchronize do
            return unless @pipeline_active

            if raw_connection.pipeline_status == PG::PQ_PIPELINE_OFF
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
              while result = raw_connection.get_result
                result.clear rescue nil
              end

              raw_connection.discard_results
              raw_connection.exit_pipeline_mode
              @pipeline_active = false
              pipeline_trace('PIPE_EXIT', @adapter, nil, nil, nil, :call_chain)

              clear_pending_results
            end
          end
        end

        def clear_pending_results(error = nil)
          synchronize do
            return if @pending_results.empty? && @in_flight_results.empty?

            error ||=
              begin
                raise ActiveRecord::StatementInvalid, "Connection was closed with pending pipeline results"
              rescue => e
                e
              end

            results_to_clear = @pending_results.dup
            in_flight_to_clear = @in_flight_results.keys.dup

            @pending_results.clear
            @in_flight_results.clear
            @flushed_through = -1
            pipeline_trace('PIPE_CLEAR', @adapter, nil, nil, nil, :call_chain)

            # Clear pending results
            results_to_clear.each do |result|
              if result.is_a?(SyncResult)
                # no-op
              else
                result.set_error(error)
              end
            end
            
            # Clear in-flight results
            in_flight_to_clear.each do |result|
              if result.is_a?(SyncResult)
                # no-op
              else
                result.set_error(error)
              end
            end
          end
        end

        def pipeline_active?
          @pipeline_active
        end

        def wait_for(result, condition = nil)
          loop do
            # Check if we need to do any work, and if so, grab exactly one result to process
            pending_result_to_process = nil
            raw_result_to_process = nil
            
            synchronize do
              if index = @pending_results.index(result)
                # Our target result is still pending - process the first result in queue
                if index >= @flushed_through
                  pipeline_trace('PIPE_FLUSH', @adapter, result, result.sql) unless result.quiet
                  flush_queries_through index
                else
                  pipeline_trace('PIPE_WAIT', @adapter, result, result.sql) unless result.quiet
                end
                
                # Dequeue exactly one result (the first one)
                pending_result_to_process, raw_result_to_process = read_and_dequeue_next_result(condition: condition)
                if pending_result_to_process && raw_result_to_process
                  @flushed_through -= 1 if @flushed_through > -1
                end
              elsif @in_flight_results.key?(result)
                # Race condition case: result is being processed by another thread
                # We need to wait for that thread to complete processing
                pipeline_trace('PIPE_WAIT_INFLIGHT', @adapter, result, result.sql) unless result.quiet
                
                # Wait on the result's own condition variable without holding connection lock
                result.instance_variable_get(:@mutex).synchronize do
                  while result.pending?
                    result.instance_variable_get(:@condition).wait
                  end
                end
                return
              else
                raise "Unknown result"
              end
            end

            # If we got a result to process, handle it outside the connection lock
            if pending_result_to_process && raw_result_to_process
              # Process result outside any locks (may trigger user callbacks)
              begin
                pending_result_to_process.set_result(raw_result_to_process)
              rescue => err
                pending_result_to_process.set_error(err)
              ensure
                # Remove from in-flight tracking
                synchronize { @in_flight_results.delete(pending_result_to_process) }
              end
              
              # If this was our target result, we're done
              return if pending_result_to_process == result
              
              # Otherwise, continue the loop to process more results
            else
              # No more results available, but our target wasn't found - this shouldn't happen
              break
            end
          end

          nil
        end

        def settle
          result = synchronize do
            pipeline_trace('PIPE_SETTLE', @adapter)
            sync_all_results
          end
          
          return result if result

          # Process all remaining results one at a time
          loop do
            pending_result_to_process = nil
            raw_result_to_process = nil
            
            synchronize do
              return unless @pending_results.any? { |r| !r.is_a?(SyncResult) && !r.ignored }

              pipeline_trace('PIPE_SETTLE', @adapter, nil, nil, nil, "sync=#{@needs_sync} pending=#{@pending_results.length}")

              send_sync if @needs_sync

              # Dequeue exactly one result (the first one)
              pending_result_to_process, raw_result_to_process = read_and_dequeue_next_result
              if pending_result_to_process && raw_result_to_process
                @flushed_through -= 1 if @flushed_through > -1
              end
            end

            # If we got a result to process, handle it outside the connection lock
            if pending_result_to_process && raw_result_to_process
              # Process result outside any locks (may trigger user callbacks)
              begin
                pending_result_to_process.set_result(raw_result_to_process)
              rescue => err
                pending_result_to_process.set_error(err)
              ensure
                # Remove from in-flight tracking
                synchronize { @in_flight_results.delete(pending_result_to_process) }
              end
            else
              # No more results available
              break
            end
          end
        end

        def pipeline_state
          synchronize do
            "#{pipeline_active? ? "active" : "inactive"} " +
              if @pending_results.empty?
                "empty"
              else
                "pending=#{@pending_results.map { |r| r.is_a?(SyncResult) ? "sync" : r.name || "unnamed" }.join(",")}"
              end
          end
        end

        def add_query(sql, binds, type_casted_binds, prepare:, name: nil, log_kwargs: nil, adapter: nil, ongoing_multi_query: false, quiet: false)
          synchronize do
            raise "Pipeline not active" unless @pipeline_active

            if raw_connection.pipeline_status == PG::PQ_PIPELINE_OFF
              pipeline_trace('PIPE_RESTORE', @adapter, nil, nil, nil, :call_chain)
              raw_connection.enter_pipeline_mode
              @pipeline_active = true
              clear_pending_results
            end

            stmt_key = nil
            if prepare
              stmt_key = @adapter.send(:prepare_statement, sql, binds, raw_connection, pipeline_result: true)
              log_kwargs[:extra] = { statement_name: stmt_key }

              raw_connection.send_query_prepared(stmt_key, type_casted_binds || [])
            else
              # Always use send_query_params in pipeline mode, even for queries without binds
              raw_connection.send_query_params(sql, type_casted_binds || [])
            end

            result = ActiveRecord::PipelineResult.new(
              self,
              sql: sql,
              name: name,
              binds: binds,
              type_casted_binds: type_casted_binds,
              adapter: adapter,
              quiet: quiet,
              stmt_key: stmt_key,
              log_kwargs: log_kwargs,
            )

            unless quiet
              if stmt_key
                pipeline_trace('PIPE_SEND_EXECUTE', @adapter, result, sql, binds, stmt_key)
              else
                pipeline_trace('PIPE_SEND', @adapter, result, sql, binds)
              end
            end

            @pending_results << result

            if ongoing_multi_query
              @needs_sync = true
            else
              send_sync
            end

            result
          end
        end

        def add_transaction_command(sql, adapter: nil)
          add_query(sql, [], [], prepare: false, name: "TRANSACTION", adapter: adapter)
        end

        def expecting_result(name, msg = nil, trace_action: 'PIPE_EXPECT', sql: nil, binds: nil, quiet: false, ongoing_multi_query: false)
          synchronize do
            raise "Pipeline not active" unless @pipeline_active

            if raw_connection.pipeline_status == PG::PQ_PIPELINE_OFF
              pipeline_trace('PIPE_RESTORE', @adapter, nil, nil, nil, :call_chain)
              raw_connection.enter_pipeline_mode
              @pipeline_active = true
              clear_pending_results
            end

            yield

            result = ActiveRecord::PipelineResult.new(
              self,
              sql: sql,
              name: name,
              binds: binds,
              type_casted_binds: nil,
              adapter: @adapter,
              quiet: quiet,
              trace_action: trace_action,
            )

            #pipeline_trace(trace_action, @adapter, result, sql, binds, msg) unless quiet

            @pending_results << result

            if ongoing_multi_query
              @needs_sync = true
            else
              send_sync
            end

            result
          end
        end

        def send_sync
          synchronize do
            raw_connection.pipeline_sync
            raw_connection.flush

            @needs_sync = false

            # sync request also implies a server flush
            @flushed_through = @pending_results.length - 1

            if TRACK_SYNCS
              result = SyncResult.new
              @pending_results << result

              result
            end
          end
        end

        def sync_all_results
          synchronize do
            pipeline_trace('PIPE_SYNC', @adapter)
            send_sync

            # Collect all results including the sync result
            collect_remaining_results
          end
        end

        def has_pending_results?
          synchronize do
            @pending_results.any?
          end
        end

        private
          def get_next_result(timeout = nil, condition: nil)
            return raw_connection.get_last_result unless TRACK_SYNCS

            prev = nil
            while true
              if condition
                condition.wait_until { raw_connection.block }
              elsif timeout
                break unless raw_connection.block(timeout)
              else
                break unless raw_connection.block
              end
              
              break unless (curr = raw_connection.get_result)

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

          def read_and_dequeue_next_result(timeout: nil, condition: nil)
            # This method must be called while holding the connection lock
            # It dequeues the next pending result and reads its data from the connection
            # Returns [pending_result, raw_result] or [nil, nil] if no more results
            
            pending_result = @pending_results.first
            return [nil, nil] unless pending_result
            
            raw_result = get_next_result(timeout, condition: condition)
            return [nil, nil] unless raw_result
            
            # Validate that the result type matches what we expected
            if (raw_result.result_status == PG::PGRES_PIPELINE_SYNC) != pending_result.is_a?(SyncResult)
              result_status_name = PG::Result.constants.grep(/^PGRES_/).select { |c| PG::Result.const_get(c) == raw_result.result_status && !c.to_s.start_with?("PGRES_POLLING_") }
              raise "Pipeline result mismatch: expected #{pending_result.class.name.gsub(/.*::/, "")}, got #{result_status_name.join("/")}"
            end
            
            # Move from pending to in-flight
            @pending_results.shift
            @in_flight_results[pending_result] = raw_result
            
            [pending_result, raw_result]
          end

          def flush_queries_through(target_index)
            #msg = "requesting flush of results for #{@pending_results[@flushed_through + 1..target_index].reject { |r| r.is_a?(SyncResult) }.size} queries"
            #pipeline_trace('PIPE_FLUSH', @adapter, nil, nil, nil, msg)
            raw_connection.send_flush_request
            raw_connection.flush
            @flushed_through = @pending_results.length - 1
          rescue => connection_error
            translated_error = @adapter.send(:translate_exception_class, connection_error, nil, nil)
            clear_pending_results(translated_error)
            raise translated_error
          end

          def collect_remaining_results(timeout = nil)
            while true
              # Check if there's still work to do
              has_pending = synchronize { @pending_results.any? }
              break unless has_pending
              
              # Dequeue and process exactly one result
              pending_result_to_process = nil
              raw_result_to_process = nil
              
              synchronize do
                pending_result_to_process, raw_result_to_process = read_and_dequeue_next_result(timeout: timeout)
                if pending_result_to_process && raw_result_to_process
                  @flushed_through -= 1 if @flushed_through > -1
                end
              end

              if pending_result_to_process && raw_result_to_process
                # Process result outside any locks (may trigger user callbacks)
                begin
                  pending_result_to_process.set_result(raw_result_to_process)
                rescue => err
                  pending_result_to_process.set_error(err)
                ensure
                  # Remove from in-flight tracking
                  synchronize { @in_flight_results.delete(pending_result_to_process) }
                end

                pending_result_to_process.result unless pending_result_to_process.ignored
              else
                # No more results available
                break
              end
            end

            raw_connection.consume_input

            if raw_connection.is_busy
              raise "still busy after collecting results?"
            end
          end
      end
    end
  end
end
