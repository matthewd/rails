# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      # Manages PostgreSQL's pipeline mode for batching multiple queries
      # in a single network round-trip.
      #
      # Pipeline mode allows sending multiple queries without waiting for
      # results, then collecting all results together. This reduces latency
      # for sequences of queries that don't depend on each other's results.
      module PipelineContext # :nodoc:
        PIPELINE_HEALTH_CHECK_SQL = ";"
        PIPELINE_SUPPORTED = PG.const_defined?(:PQ_PIPELINE_OFF)
        private_constant :PIPELINE_HEALTH_CHECK_SQL, :PIPELINE_SUPPORTED

        # Marker for sync points in the pipeline.
        class SyncIntent # :nodoc:
        end

        class ReplayWindow # :nodoc:
          def initialize(retry_required: [], replay_safe: [])
            @retry_required = retry_required
            @replay_safe = replay_safe
          end

          def each(&block)
            return enum_for(__method__) unless block

            operations.each { |operation| block.call(operation) unless operation.is_a?(SyncIntent) }
          end

          def first
            each.first
          end

          def next_operation
            @retry_required.first || @replay_safe.first
          end

          def next_operation_retry_required?
            @retry_required.any?
          end

          def shift_operation
            (@retry_required.empty? ? @replay_safe : @retry_required).shift
          end

          def intents_requiring_retry
            @retry_required.reject { |operation| operation.is_a?(SyncIntent) }
          end

          def frontier_group_intents
            return [] if @retry_required.empty?

            group = []
            operations.each do |operation|
              if operation.is_a?(SyncIntent)
                return group if group.any?
              else
                group << operation
              end
            end
            group
          end

          def advance_retry_required_through(intent)
            index = @replay_safe.index(intent)
            @retry_required.concat(@replay_safe.shift(index + 1)) if index
          end

          def prepend!(window)
            if @retry_required.any?
              @retry_required = window.operations + @retry_required
            else
              @retry_required = window.retry_required.dup
              @replay_safe = window.replay_safe + @replay_safe
            end
            self
          end

          def to_a
            each.to_a
          end

          protected
            attr_reader :retry_required, :replay_safe

            def operations
              @retry_required + @replay_safe
            end
        end
        private_constant :ReplayWindow

        class ConnectionProbeIntent # :nodoc:
          attr_reader :error

          def processed_sql
            PIPELINE_HEALTH_CHECK_SQL
          end

          def type_casted_binds
            []
          end

          def binds
            []
          end

          def deliver_result(result)
            @raw_result = result
          end

          def deliver_failure(error)
            @error = error
          end

          def clear_raw_result
            @raw_result&.clear
            @raw_result = nil
          end
        end
        private_constant :ConnectionProbeIntent

        def supports_pipeline?
          PIPELINE_SUPPORTED
        end

        def pipeline_active?
          @lock.synchronize do
            supports_pipeline? && connected? && @raw_connection.pipeline_status != PG::PQ_PIPELINE_OFF
          end
        end

        def pipeline_pending?
          @lock.synchronize do
            @pending_intents ||= []
            @pending_intents.any?
          end
        end

        def active_pipeline_connection?
          probe = nil

          @lock.synchronize do
            return false unless pipeline_active?

            @pending_intents ||= []

            if @pending_intents.empty? || @pending_intents.last.is_a?(SyncIntent)
              pipeline_sync
            else
              probe = ConnectionProbeIntent.new
              pipeline_add_query(probe)
              @raw_connection.send_flush_request
              @raw_connection.flush
            end

            drain_pipeline
            return false if probe&.error

            connected? && !@needs_reconnect
          end
        rescue PG::Error, IOError, SystemCallError
          discard_pipeline_buffer
          @pipeline_connection_error_context = nil
          false
        ensure
          if probe
            @lock.synchronize do
              @pending_intents&.delete(probe)
              probe.clear_raw_result
            end
          end
          pool.release_connection_if_unheld(self)
        end

        def enter_pipeline_mode
          @lock.synchronize do
            raise NotImplementedError, "PostgreSQL pipeline mode is unavailable" unless supports_pipeline?
            return if pipeline_active?
            raise "Cannot enter pipeline mode: pipelining is locked" if @pipelining_locked

            unless connected?
              raise ActiveRecord::ConnectionFailed, "Connection is not usable while entering pipeline mode"
            end

            @raw_connection.enter_pipeline_mode
          end
        end

        def pipeline_sync
          @lock.synchronize do
            return unless pipeline_active?

            @pending_intents ||= []

            # Probe the connection before recording a sync marker. Flush
            # sends any buffered query data, then consume_input reads from
            # the socket - either will raise on a dead connection while we
            # still know no sync has been sent. (Flush alone isn't enough:
            # libpq may have eagerly sent query data during send_query_params,
            # leaving the write buffer empty. consume_input always has
            # something to check on the read side.)
            #
            # If these probes succeed, we record the SyncIntent - the server
            # may have received the queries. Then pipeline_sync sends the
            # actual sync message. If *that* fails, the marker stays:
            # conservatively assuming the sync might have reached the wire.
            @raw_connection.flush
            @raw_connection.consume_input
            @pending_intents << SyncIntent.new
            @raw_connection.pipeline_sync
          end
        end

        def exit_pipeline_mode
          @lock.synchronize do
            return unless pipeline_active?
            raise "Cannot exit pipeline mode: pipelining is locked" if @pipelining_locked

            begin
              begin
                flush_pipeline if connected?
              ensure
                abandon_pipelined_intents
              end
            ensure
              if connected?
                begin
                  @raw_connection.exit_pipeline_mode
                rescue PG::Error
                  @raw_connection.close
                end
              end
            end
          end
        end

        # Add a query intent to the pipeline.
        # The intent's raw_result will be populated when the pipeline is flushed.
        def pipeline_add_query(intent)
          @lock.synchronize do
            raise "Pipeline mode not active" unless pipeline_active?

            @pending_intents ||= []

            # Send the query to the pipeline.
            # Always use send_query_params in pipeline mode (even with empty binds array).
            @raw_connection.send_query_params(
              intent.processed_sql,
              intent.type_casted_binds || []
            )

            # Only add to pending list after successful send to avoid misalignment
            # if send_query_params raises an exception.
            @pending_intents << intent
          end

          intent
        end

        # Flush pending queries and collect results.
        #
        # Connection errors during sync/drain are handled with transparent
        # replay when all outstanding intents are eligible, otherwise intents
        # are abandoned with appropriate terminal states.
        def flush_pipeline(waiting_on: nil)
          @lock.synchronize do
            return unless pipeline_active?
            return unless pipeline_pending?

            # Track which intent was at the head of the last replay
            # attempt. If we come back around and the same intent is
            # still leading, we're not making progress - give up.
            # If the head has advanced (some intents succeeded), that's
            # genuine progress and worth another attempt.
            last_replayed_head = nil

            loop do
              sync_error = nil
              begin
                pipeline_sync
              rescue PG::Error, IOError, SystemCallError => error
                sync_error = error
              end

              if sync_error
                begin
                  consume_pipeline
                rescue PG::Error, IOError, SystemCallError => error
                  sync_error = error
                end

                if replayable = recover_pipeline_window(sync_error, last_replayed_head)
                  last_replayed_head = replayable.first
                  next if replay_pipeline_intents(replayable)
                end

                return
              end

              begin
                drain_pipeline
              rescue PG::Error, IOError, SystemCallError => error
                if replayable = recover_pipeline_window(error, last_replayed_head)
                  last_replayed_head = replayable.first
                  next if replay_pipeline_intents(replayable)
                end
              end

              return
            end
          end
        ensure
          pool.release_connection_if_unheld(self) unless waiting_on
        end

        def drain_pipeline
          @lock.synchronize do
            @pending_intents ||= []

            while @pending_intents.any?
              pending_count = @pending_intents.length
              buffer_count = pipeline_buffer.length

              consume_pipeline
              break if @pending_intents.empty?

              next if @pending_intents.length != pending_count || pipeline_buffer.length != buffer_count
              break unless @raw_connection.is_busy

              @raw_connection.block
            end
          end
        end

        def consume_pipeline
          @lock.synchronize do
            @pending_intents ||= []
            return if @pending_intents.empty?

            # Results are tentative until the server confirms the sync point.
            # Keep pending intents in place while buffering so a connection
            # failure before PGRES_PIPELINE_SYNC leaves the whole group
            # available for replay / unknown-fate classification.
            begin
              while intent = @pending_intents[pipeline_buffer.length]
                if intent.is_a?(SyncIntent)
                  sync_result = get_available_pipeline_sync_result
                  return unless sync_result

                  sync_result.check
                  unless sync_result.result_status == PG::PGRES_PIPELINE_SYNC
                    raise "BUG: expected pipeline sync result, got #{sync_result.result_status}"
                  end

                  confirmed_intents = @pending_intents.shift(pipeline_buffer.length + 1)
                  if confirmed_intents.include?(@pipeline_replay_retry_required_through)
                    @pipeline_replay_retry_required_through = nil
                  end
                  sync_result.clear
                  deliver_pipeline_buffer
                  next
                end

                raw_result = get_available_pipeline_query_result
                return unless raw_result

                if intent.is_a?(ConnectionProbeIntent)
                  deliver_pipeline_probe_result(intent, raw_result)
                  next
                end

                if raw_result.result_status == PG::PGRES_PIPELINE_ABORTED
                  raw_result.clear
                  pipeline_buffer << [:not_run, intent, :server_aborted, take_notice_receiver_warnings]
                  next
                end

                begin
                  raw_result.check
                rescue => error
                  translated = translate_exception_class(error, intent.processed_sql, intent.binds)
                  if retryable_connection_error?(translated)
                    raise error
                  end

                  pipeline_buffer << [:failure, intent, translated, take_notice_receiver_warnings, raw_result]
                  next
                end

                pipeline_buffer << [:result, intent, raw_result, take_notice_receiver_warnings]
              end
            rescue PG::Error, IOError, SystemCallError => error
              failure_intent = intent unless intent.is_a?(SyncIntent)
              @pipeline_connection_error_context = [error, failure_intent]
              discard_pipeline_buffer
              raise
            end
          end
        end

        private
          def deliver_pipeline_probe_result(intent, raw_result)
            @pending_intents.delete_at(pipeline_buffer.length)
            take_notice_receiver_warnings

            if raw_result.result_status == PG::PGRES_PIPELINE_ABORTED
              intent.deliver_result(raw_result)
            else
              begin
                raw_result.check
              rescue => error
                translated = translate_exception_class(error, intent.processed_sql, intent.binds)
                if retryable_connection_error?(translated)
                  raw_result.clear
                  discard_pipeline_buffer
                  raise error
                end

                raw_result.clear
                intent.deliver_failure(translated)
              else
                intent.deliver_result(raw_result)
              end
            end
          end

          def recover_pipeline_enqueue(error, replay_window, allow_recovery:)
            if replay_window
              pending_queries = @pending_intents&.any? { |intent| !intent.is_a?(SyncIntent) }
              @raw_connection&.close
              accepted = abandon_pipelined_intents(error, allow_recovery: allow_recovery)

              unless allow_recovery
                settle_pipeline_replay_window(replay_window, error)
                return
              end

              if pending_queries && !accepted
                settle_pipeline_replay_window(replay_window, error)
                raise error
              end

              replay_window.prepend!(accepted) if accepted
              return replay_window
            end

            pending_queries = @pending_intents&.any? { |intent| !intent.is_a?(SyncIntent) }
            replayable = abandon_pipelined_intents(error, allow_recovery: allow_recovery)

            if allow_recovery && (replayable || !pending_queries)
              replayable || ReplayWindow.new
            end
          end

          def replay_pipeline_intents(window)
            reconnect!(restore_transactions: true)
            enter_pipeline_mode
            submit_pipeline_replay(window)
            true
          rescue Exception => error
            failure_intent = take_pipeline_connection_error_intent(error)
            translated = if error.is_a?(PG::Error) || error.is_a?(IOError) || error.is_a?(SystemCallError)
              translate_pipeline_connection_error(error)
            else
              error
            end
            cause = translated.cause
            server_fatal_intent = failure_intent if cause.respond_to?(:result) && connection_terminating_severity?(cause.result)

            @raw_connection&.close
            abandon_pipelined_intents(translated, server_fatal_intent: server_fatal_intent)
            settle_pipeline_replay_window(window, translated)
            raise error unless translated.is_a?(ActiveRecord::ConnectionNotEstablished) || translated.is_a?(ActiveRecord::ConnectionFailed)

            false
          end

          def submit_pipeline_replay(window)
            while operation = window.next_operation
              retry_required = window.next_operation_retry_required?
              if operation.is_a?(SyncIntent)
                window.shift_operation
                pipeline_sync
              else
                pipeline_add_query(operation)
                @pipeline_replay_retry_required_through = operation if retry_required
                window.shift_operation
              end
            end
          end

          def settle_pipeline_replay_window(window, error, no_progress: false)
            retry_required = window.intents_requiring_retry
            window.each do |intent|
              if no_progress || retry_required.include?(intent)
                intent.retry_budget&.reconnect_consumed! if no_progress
                intent.deliver_failure(error)
              else
                intent.deliver_not_run(reason: :unsynced)
              end
            end
          end

          def recover_pipeline_window(error, last_replayed_head)
            failure_intent = take_pipeline_connection_error_intent(error)
            translated = translate_pipeline_connection_error(error)
            cause = translated.cause
            server_fatal_intent = failure_intent if cause.respond_to?(:result) && connection_terminating_severity?(cause.result)
            allow_recovery = retryable_connection_error?(translated) && reconnect_can_restore_state?

            abandon_pipelined_intents(
              translated,
              server_fatal_intent: server_fatal_intent,
              allow_recovery: allow_recovery,
              last_replayed_head: last_replayed_head
            )
          end

          def take_pipeline_connection_error_intent(error)
            context, @pipeline_connection_error_context = @pipeline_connection_error_context, nil
            context.last if context&.first.equal?(error)
          end

          def translate_pipeline_connection_error(error)
            if error.is_a?(PG::Error)
              return translate_exception_class(prefer_notice_receiver_fatal_error(error), nil, nil)
            end

            raise error
          rescue IOError, SystemCallError => raised
            begin
              raise ActiveRecord::ConnectionFailed.new(raised, connection_pool: @pool)
            rescue ActiveRecord::ConnectionFailed => translated
              translated
            end
          end

          def prefer_notice_receiver_fatal_error(error)
            if error.respond_to?(:result) && error.result
              consume_notice_receiver_fatal_error
              return error
            end
            return error unless error.class == PG::Error || error.is_a?(PG::ConnectionBad)

            consume_notice_receiver_fatal_error || error
          end

          def pipeline_buffer
            @pipeline_buffer ||= []
          end

          def replace_pipeline_result_buffer(result)
            @pipeline_result_buffer&.clear
            @pipeline_result_buffer = result
          end

          def take_pipeline_result_buffer
            @pipeline_result_buffer.tap do
              @pipeline_result_buffer = nil
            end
          end

          def discard_pipeline_result_buffer
            @pipeline_result_buffer&.clear
            @pipeline_result_buffer = nil
          end

          # Return input errors instead of raising immediately: libpq may
          # have already parsed a result, including a typed FATAL result,
          # before reporting that the socket has closed.
          def consume_pipeline_input
            @raw_connection.consume_input
            nil
          rescue PG::Error, IOError, SystemCallError => error
            error
          end

          # In pipeline mode libpq returns nil between command results.
          # Skip one such boundary while results remain immediately readable,
          # but stop before any get_result call that PQisBusy says would block.
          def get_available_pipeline_query_result
            skipped_boundary = false

            loop do
              input_error = consume_pipeline_input
              busy = @raw_connection.is_busy

              if busy
                raise input_error if input_error
                return
              end

              result = @raw_connection.get_result
              if result
                replace_pipeline_result_buffer(result)
                return take_pipeline_result_buffer if result.result_status == PG::PGRES_FATAL_ERROR &&
                  connection_terminating_severity?(result)

                skipped_boundary = false
                next
              end

              return take_pipeline_result_buffer if @pipeline_result_buffer

              raise input_error if input_error
              return if skipped_boundary

              skipped_boundary = true
            end
          end

          def get_available_pipeline_sync_result
            skipped_boundary = false

            loop do
              input_error = consume_pipeline_input
              busy = @raw_connection.is_busy

              if busy
                raise input_error if input_error
                return
              end

              result = @raw_connection.get_result
              return result if result

              raise input_error if input_error
              return if skipped_boundary

              skipped_boundary = true
            end
          end

          def discard_pipeline_buffer
            discard_pipeline_result_buffer

            pipeline_buffer.each do |kind, _intent, value, _warnings, raw_result|
              case kind
              when :result
                value.clear
              when :failure
                raw_result&.clear
              end
            end
            pipeline_buffer.clear
          end

          def deliver_pipeline_buffer
            delivery_error = nil

            pipeline_buffer.each do |kind, intent, value, warnings, _raw_result|
              case kind
              when :result
                if intent.notification_payload && value
                  intent.notification_payload[:affected_rows] = value.cmd_tuples
                  intent.notification_payload[:row_count] = value.ntuples
                end
                intent.deliver_result(value, warnings: warnings)
              when :failure
                intent.deliver_failure(value, warnings: warnings)
              when :not_run
                intent.deliver_not_run(reason: value, warnings: warnings)
              end
            rescue Exception => error
              delivery_error ||= error
            end

            raise delivery_error if delivery_error
          ensure
            pipeline_buffer.clear
          end

          def take_notice_receiver_warnings
            @notice_receiver_sql_warnings.tap do
              @notice_receiver_sql_warnings = []
            end
          end

          # Replaying the full window requires permission from every uncertain
          # intent, but retry budget advances only when a sync confirms progress.
          # Charging later groups now would let one connection loss exhaust the
          # whole buffer, preventing recovery from a later loss after earlier
          # groups had been confirmed. Later groups therefore retain their budget
          # until they reach the frontier.
          def consume_pipeline_retry_budgets(error, window)
            intents_requiring_retry = window.intents_requiring_retry
            return true if intents_requiring_retry.empty?
            return false unless intents_requiring_retry.all?(&:allow_retry)
            return false unless retryable_connection_error?(error)

            frontier_group = window.frontier_group_intents
            frontier_budgets = frontier_group.filter_map do |intent|
              next unless intents_requiring_retry.include?(intent)

              intent.retry_budget ||= build_retry_budget(
                allow_retry: intent.allow_retry,
                reconnectable: reconnect_can_restore_state?
              )
            end
            retry_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            return false unless frontier_budgets.all? { |budget| budget.reconnectable? && budget.available?(retry_at) }

            frontier_budgets.each { |budget| budget.consume(retry_at) }
            frontier_group.each do |intent|
              intent.retry_budget ||= build_retry_budget(
                allow_retry: intent.allow_retry,
                reconnectable: reconnect_can_restore_state?
              )
              intent.retry_budget.reconnect_consumed!
            end
            true
          end

          # Classify and deliver terminal states to all pending intents after
          # a connection failure, based on sync boundaries.
          #
          # When +allow_recovery+ is true, intents whose execution might have
          # committed require retry budget. Work from an unsynced group, and
          # work skipped after a server FATAL, can replay without permission.
          # Returns the ordered unresolved window for replay when every
          # permission-requiring intent is eligible; otherwise delivers
          # terminal states.
          #
          # +last_replayed_head+ gates progress: if the first intent in the
          # replay list is the same as the last attempt, we're not making
          # progress and fall through to deliver terminal states instead.
          def abandon_pipelined_intents(connection_error = nil, server_fatal_intent: nil,
            allow_recovery: false, last_replayed_head: nil)
            discard_pipeline_buffer

            intents = @pending_intents
            @pending_intents = []
            return unless intents&.any?

            replayable = build_replay_window(intents, server_fatal_intent)
            replayable.advance_retry_required_through(@pipeline_replay_retry_required_through)
            @pipeline_replay_retry_required_through = nil
            no_progress = last_replayed_head && replayable.first.equal?(last_replayed_head)
            if allow_recovery && !no_progress && consume_pipeline_retry_budgets(connection_error, replayable)
              replayable.each(&:reset_for_retry)
              return replayable
            end

            error = connection_error || ActiveRecord::ConnectionFailed.new("Connection lost during pipeline execution")
            settle_pipeline_replay_window(replayable, error, no_progress: no_progress)
            nil
          end

          def build_replay_window(intents, server_fatal_intent = nil)
            retry_required_index = if server_fatal_intent
              intents.index(server_fatal_intent)
            elsif (last_sync_index = intents.rindex { |intent| intent.is_a?(SyncIntent) })
              (last_sync_index - 1).downto(0).find { |index| !intents[index].is_a?(SyncIntent) }
            end

            if retry_required_index
              ReplayWindow.new(
                retry_required: intents[0..retry_required_index],
                replay_safe: intents[(retry_required_index + 1)..] || []
              )
            else
              ReplayWindow.new(replay_safe: intents.dup)
            end
          end
      end
    end
  end
end
