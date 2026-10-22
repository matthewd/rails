# frozen_string_literal: true

require "active_support/testing/parallelization"

module ActiveRecord
  module TestDatabases # :nodoc:
    ActiveSupport::Testing::Parallelization.before_fork_hook do
      if ActiveSupport.parallelize_test_databases
        ActiveRecord::Base.connection_handler.clear_all_connections!
      end
    end

    ActiveSupport::Testing::Parallelization.after_fork_hook do |i|
      if ActiveSupport.parallelize_test_databases
        create_and_load_schema(i, env_name: ActiveRecord::ConnectionHandling::DEFAULT_ENV.call)
      end
    end

    ActiveSupport::Testing::Parallelization.run_cleanup_hook do
      ActiveRecord::Base.connection_handler.each_connection_pool.each(&:discard!)
    end

    def self.create_and_load_schema(i, env_name:)
      old, ENV["VERBOSE"] = ENV["VERBOSE"], "false"

      # Database configs may be frozen (e.g. once shared with another Ractor),
      # so each renamed database gets a new config object, which replaces the
      # original in the configurations and in already established pools.
      replacements = {}.compare_by_identity
      ActiveRecord::Base.configurations = ActiveRecord::Base.configurations.configurations.map do |db_config|
        next db_config unless db_config.env_name == env_name

        replacements[db_config] = db_config.dup.tap { |renamed| renamed._database = "#{db_config.database}_#{i}" }
      end
      ActiveRecord::ConnectionAdapters::PoolConfig.replace_db_configs!(replacements)

      ActiveRecord::Base.configurations.configs_for(env_name: env_name).each do |db_config|
        ActiveRecord::Tasks::DatabaseTasks.reconstruct_from_schema(db_config, nil)
      end
    ensure
      ActiveRecord::Base.establish_connection
      ENV["VERBOSE"] = old
    end
  end
end
