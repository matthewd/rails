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

        def wait_for(target_result, condition = nil)
          loop do
            result, raw_data = synchronize do
              if @pending_results.include?(target_result)
                prepare_to_wait_for(target_result)
                read_and_dequeue_next_result(condition: condition)
              elsif @in_flight_results.key?(target_result)
                wait_for_in_flight_result(target_result)
                return
              else
                raise "Unknown result"
              end
            end

            if result && raw_data
              process_result(result, raw_data)
              return if result == target_result
            else
              break # No more results available
            end
          end
        end

        def settle
          return sync_all_results_if_possible || process_remaining_results
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
            # Must be called while holding connection lock
            # Returns [result, raw_data] or [nil, nil] if no more results
            
            result = @pending_results.first
            return [nil, nil] unless result
            
            raw_data = get_next_result(timeout, condition: condition)
            return [nil, nil] unless raw_data
            
            # Validate result type matches expectation
            sync_result = (raw_data.result_status == PG::PGRES_PIPELINE_SYNC)
            if sync_result != result.is_a?(SyncResult)
              status_names = PG::Result.constants.grep(/^PGRES_/).select do |c|
                PG::Result.const_get(c) == raw_data.result_status && !c.to_s.start_with?("PGRES_POLLING_")
              end
              raise "Pipeline result mismatch: expected #{result.class.name.gsub(/.*::/, "")}, got #{status_names.join("/")}"
            end
            
            # Move from pending to in-flight and update bookkeeping
            @pending_results.shift
            @in_flight_results[result] = raw_data
            @flushed_through -= 1 if @flushed_through > -1
            
            [result, raw_data]
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

          def prepare_to_wait_for(target_result)
            target_index = @pending_results.index(target_result)
            if target_index >= @flushed_through
              pipeline_trace('PIPE_FLUSH', @adapter, target_result, target_result.sql) unless target_result.quiet
              flush_queries_through target_index
            else
              pipeline_trace('PIPE_WAIT', @adapter, target_result, target_result.sql) unless target_result.quiet
            end
          end
          
          def wait_for_in_flight_result(target_result)
            pipeline_trace('PIPE_WAIT_INFLIGHT', @adapter, target_result, target_result.sql) unless target_result.quiet
            
            target_result.instance_variable_get(:@mutex).synchronize do
              while target_result.pending?
                target_result.instance_variable_get(:@condition).wait
              end
            end
          end
          
          def sync_all_results_if_possible
            synchronize do
              pipeline_trace('PIPE_SETTLE', @adapter)
              sync_all_results
            end
          end
          
          def process_remaining_results
            loop do
              result, raw_data = synchronize do
                return unless has_readable_results?
                
                pipeline_trace('PIPE_SETTLE', @adapter, nil, nil, nil, "sync=#{@needs_sync} pending=#{@pending_results.length}")
                send_sync if @needs_sync
                read_and_dequeue_next_result
              end

              if result && raw_data
                process_result(result, raw_data)
              else
                break
              end
            end
          end
          
          def has_readable_results?
            @pending_results.any? { |r| !r.is_a?(SyncResult) && !r.ignored }
          end

          def process_result(result, raw_data)
            # Process result outside any locks (may trigger user callbacks)
            begin
              result.set_result(raw_data)
            rescue => err
              result.set_error(err)
            ensure
              # Remove from in-flight tracking
              synchronize { @in_flight_results.delete(result) }
            end
          end

          def collect_remaining_results(timeout = nil)
            process_all_results(timeout: timeout)
            finalize_connection_state
          end
          
          def process_all_results(timeout: nil)
            loop do
              break unless synchronize { @pending_results.any? }
              
              result, raw_data = synchronize { read_and_dequeue_next_result(timeout: timeout) }
              
              if result && raw_data
                process_result(result, raw_data)
                result.result unless result.ignored
              else
                break
              end
            end
          end
          
          def finalize_connection_state
            raw_connection.consume_input

            if raw_connection.is_busy
              raise "still busy after collecting results?"
            end
          end
      end
    end
  end
end
