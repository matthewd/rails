# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    # = Active Record Connection Adapters \Savepoints
    module Savepoints
      def current_savepoint_name
        current_transaction.savepoint_name
      end

      def create_savepoint(name = current_savepoint_name, pipeline_result: false)
        internal_execute("SAVEPOINT #{name}", "TRANSACTION", materialize_transactions: false, pipeline_result: pipeline_result)
      end

      def exec_rollback_to_savepoint(name = current_savepoint_name)
        internal_execute("ROLLBACK TO SAVEPOINT #{name}", "TRANSACTION", materialize_transactions: true)
      end

      def release_savepoint(name = current_savepoint_name)
        internal_execute("RELEASE SAVEPOINT #{name}", "TRANSACTION", materialize_transactions: true)
      end
    end
  end
end
