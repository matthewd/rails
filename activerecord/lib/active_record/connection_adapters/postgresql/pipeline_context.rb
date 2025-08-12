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
            status = raw_connection.status
            is_busy = raw_connection.is_busy
            transaction_status = raw_connection.transaction_status
            $stderr.puts "ENTER_PIPELINE_MODE: connection_status=#{status}, is_busy=#{is_busy}, transaction_status=#{transaction_status}" if status != 0 || is_busy || transaction_status != 0
            if status == PG::CONNECTION_BAD
              raise "DEBUG: Connection already bad before enter_pipeline_mode! status=#{status}, transaction_status=#{transaction_status}"
            end
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
            return if @pending_results.empty?

            error ||=
              begin
                raise ActiveRecord::StatementInvalid, "Connection was closed with pending pipeline results"
              rescue => e
                e
              end

            results_to_clear = @pending_results.dup

            @pending_results.clear
            @flushed_through = -1
            pipeline_trace('PIPE_CLEAR', @adapter, nil, nil, nil, :call_chain)

            results_to_clear.each do |result|
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

        def wait_for(result)
          synchronize do
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

        def settle
          synchronize do
            pipeline_trace('PIPE_SETTLE', @adapter)
            return sync_all_results


            last_readable_result = @pending_results.rindex { |r| !r.is_a?(SyncResult) && !r.ignored }

            pipeline_trace('PIPE_SETTLE', @adapter, nil, nil, nil, "sync=#{@needs_sync} readable=#{last_readable_result&.+(1)} pending=#{@pending_results.length}")

            send_sync if @needs_sync

            if last_readable_result
              collect_results_through(last_readable_result)
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
            )

            pipeline_trace(trace_action, @adapter, result, sql, binds, msg) unless quiet

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
          $stderr.puts "sync_all_results: enter"
          synchronize do
            $stderr.puts "sync_all_results: synchronize"
            pipeline_trace('PIPE_SYNC', @adapter)
            $stderr.puts "sync_all_results: send_sync"
            send_sync

            # Collect all results including the sync result
            $stderr.puts "sync_all_results: collect_remaining_results"
            collect_remaining_results
            $stderr.puts "sync_all_results: done"
          end
        rescue => ex
          $stderr.puts "sync_all_results: error"
          $stderr.puts "sync_all_results: error: #{ex.inspect}"
          raise
        end

        def has_pending_results?
          synchronize do
            @pending_results.any?
          end
        end

        private
          def get_next_result(timeout = nil)
            return raw_connection.get_last_result unless TRACK_SYNCS

            prev = nil
            while (timeout ? raw_connection.block(timeout) : raw_connection.block) && (curr = raw_connection.get_result)
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
            raw_connection.send_flush_request
            raw_connection.flush
            @flushed_through = @pending_results.length - 1
          rescue => connection_error
            translated_error = @adapter.send(:translate_exception_class, connection_error, nil, nil)
            clear_pending_results(translated_error)
            raise translated_error
          end

          def collect_results_through(target_index, timeout: nil)
            n = target_index

            while n >= 0 && pending_result = @pending_results.first
              begin
                #pipeline_trace('PIPE_GET', @adapter, pending_result, (pending_result.sql if pending_result.respond_to?(:sql)))

                raw_result = get_next_result(timeout)
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

          def collect_remaining_results(timeout = nil)
            while pending_result = @pending_results.first
              collect_results_through(0, timeout: timeout)

              pending_result.result unless pending_result.ignored
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
