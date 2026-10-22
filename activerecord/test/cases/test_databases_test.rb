# frozen_string_literal: true

require "cases/helper"
require "active_record/test_databases"

class TestDatabasesTest < ActiveRecord::TestCase
  unless in_memory_db?
    def test_databases_are_created
      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations, {
        "arunit" => {
          "primary" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" }
        }
      }

      base_db_config = ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary")
      expected_database = "#{base_db_config.database}_2"

      ActiveRecord::Tasks::DatabaseTasks.stub(:reconstruct_from_schema, ->(db_config, _) {
        assert_equal expected_database, db_config.database
      }) do
        ActiveRecord::TestDatabases.create_and_load_schema(2, env_name: "arunit")
      end
    ensure
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end

    def test_create_databases_after_fork
      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations, {
        "arunit" => {
          "primary" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" }
        }
      }

      idx = 42
      base_db_config = ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary")
      expected_database = "#{base_db_config.database}_#{idx}"

      ActiveRecord::Tasks::DatabaseTasks.stub(:reconstruct_from_schema, ->(db_config, _) {
        assert_equal expected_database, db_config.database
      }) do
        ActiveSupport::Testing::Parallelization.after_fork_hooks.each { |cb| cb.call(idx) }
      end

      # Updates the database configuration
      assert_equal expected_database, ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary").database
    ensure
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end

    def test_create_databases_skipped_if_parallelize_test_databases_is_false
      parallelize_databases = ActiveSupport.parallelize_test_databases
      ActiveSupport.parallelize_test_databases = false

      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations, {
        "arunit" => {
          "primary" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" }
        }
      }

      idx = 42
      base_db_config = ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary")
      expected_database = "#{base_db_config.database}"

      ActiveSupport::Testing::Parallelization.after_fork_hooks.each { |cb| cb.call(idx) }

      # In this case, there should be no updates
      assert_equal expected_database, ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary").database
    ensure
      ActiveSupport.parallelize_test_databases = parallelize_databases
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end

    def test_order_of_configurations_isnt_changed_by_test_databases
      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations, {
        "arunit" => {
          "primary" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" },
          "replica" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" }
        }
      }

      idx = 42
      base_configs_order = ActiveRecord::Base.configurations.configs_for(env_name: "arunit").map(&:name)

      ActiveRecord::Tasks::DatabaseTasks.stub(:reconstruct_from_schema, ->(db_config, _) {
        assert_equal base_configs_order, ActiveRecord::Base.configurations.configs_for(env_name: "arunit").map(&:name)
      }) do
        ActiveSupport::Testing::Parallelization.after_fork_hooks.each { |cb| cb.call(idx) }
      end
    ensure
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end

    class CustomUrlConfig < ActiveRecord::DatabaseConfigurations::UrlConfig
    end

    def test_create_databases_after_fork_with_frozen_configs
      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      primary_db_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
        "arunit", "primary", { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" }
      ).freeze
      other_db_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
        "other", "primary", { "adapter" => "sqlite3", "database" => "test/db/other.sqlite3" }
      ).freeze
      replica_db_config = CustomUrlConfig.new(
        "arunit", "replica", "sqlite3:test/db/primary.sqlite3", { "replica" => true, "custom" => "kept" }
      ).freeze
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations,
        [primary_db_config, other_db_config, replica_db_config]

      idx = 42
      reconstructed = []
      ActiveRecord::Tasks::DatabaseTasks.stub(:reconstruct_from_schema, ->(db_config, _) { reconstructed << db_config }) do
        ActiveSupport::Testing::Parallelization.after_fork_hooks.each { |cb| cb.call(idx) }
      end

      renamed_primary, unchanged_other, renamed_replica = ActiveRecord::Base.configurations.configurations

      assert_equal [["arunit", "primary"], ["other", "primary"], ["arunit", "replica"]],
        ActiveRecord::Base.configurations.configurations.map { |db_config| [db_config.env_name, db_config.name] }
      assert_same other_db_config, unchanged_other

      assert_equal "test/db/primary.sqlite3_#{idx}", renamed_primary.database
      assert_equal "test/db/primary.sqlite3_#{idx}", renamed_replica.database
      assert_instance_of ActiveRecord::DatabaseConfigurations::HashConfig, renamed_primary
      assert_instance_of CustomUrlConfig, renamed_replica
      assert_equal replica_db_config.url, renamed_replica.url
      assert_equal "kept", renamed_replica.configuration_hash[:custom]
      assert renamed_replica.replica?

      assert_equal [renamed_primary], reconstructed
      assert_same renamed_primary, reconstructed.first
      assert_same renamed_primary, ActiveRecord::Base.configurations.resolve(:arunit)
      assert_same renamed_primary, ActiveRecord::Base.configurations.resolve(:arunit)

      assert_equal "test/db/primary.sqlite3", primary_db_config.database
      assert_equal "test/db/primary.sqlite3", replica_db_config.database
    ensure
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end

    class AnimalsRecord < ActiveRecord::Base
      self.abstract_class = true
    end

    def test_create_databases_after_fork_renames_databases_of_established_pools
      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations, {
        "arunit" => {
          "primary" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" },
          "animals" => { "adapter" => "sqlite3", "database" => "test/db/animals.sqlite3" },
          "animals_replica" => { "adapter" => "sqlite3", "database" => "test/db/animals.sqlite3", "replica" => true },
          "animals_shard_one" => { "adapter" => "sqlite3", "database" => "test/db/animals_shard_one.sqlite3" }
        }
      }

      pool_keys = [[:writing, :default], [:reading, :default], [:writing, :shard_one]]
      custom_handler = ActiveRecord::ConnectionAdapters::ConnectionHandler.new
      idx = 42

      without_ractor_proxy do
        AnimalsRecord.connects_to shards: {
          default: { writing: :animals, reading: :animals_replica },
          shard_one: { writing: :animals_shard_one }
        }

        animals_db_config = ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "animals")
        independent_db_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
          "arunit", "animals", animals_db_config.configuration_hash
        )
        custom_handler.establish_connection(animals_db_config, owner_name: "custom_animals")
        custom_handler.establish_connection(independent_db_config, owner_name: "independent_animals")

        ActiveRecord::Tasks::DatabaseTasks.stub(:reconstruct_from_schema, ->(*) { }) do
          ActiveSupport::Testing::Parallelization.after_fork_hooks.each { |cb| cb.call(idx) }
        end

        databases = pool_keys.map do |role, shard|
          ActiveRecord::Base.connection_handler.retrieve_connection_pool(AnimalsRecord.name, role: role, shard: shard).db_config.database
        end

        assert_equal [
          "test/db/animals.sqlite3_#{idx}",
          "test/db/animals.sqlite3_#{idx}",
          "test/db/animals_shard_one.sqlite3_#{idx}"
        ], databases
        assert_same ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "animals"),
          custom_handler.retrieve_connection_pool("custom_animals").db_config
        assert_same independent_db_config, custom_handler.retrieve_connection_pool("independent_animals").db_config
        assert_equal "test/db/animals.sqlite3", independent_db_config.database
      end
    ensure
      without_ractor_proxy do
        pool_keys&.each do |role, shard|
          ActiveRecord::Base.connection_handler.remove_connection_pool(AnimalsRecord.name, role: role, shard: shard)
        end
      end
      custom_handler&.clear_all_connections!
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end

    def test_create_databases_after_fork_with_replica
      previous_env, ENV["RAILS_ENV"] = ENV["RAILS_ENV"], "arunit"
      prev_configs, ActiveRecord::Base.configurations = ActiveRecord::Base.configurations, {
        "arunit" => {
          "primary" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3" },
          "replica" => { "adapter" => "sqlite3", "database" => "test/db/primary.sqlite3", "replica" => true }
        }
      }

      idx = 42
      primary_db_config = ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary")
      expected_primary_database = "#{primary_db_config.database}_#{idx}"
      replica_db_config = ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "replica", include_hidden: true)
      expected_replica_database = "#{replica_db_config.database}_#{idx}"

      ActiveRecord::Tasks::DatabaseTasks.stub(:reconstruct_from_schema, ->(db_config, _) {
        assert_equal expected_primary_database, db_config.database
      }) do
        ActiveSupport::Testing::Parallelization.after_fork_hooks.each { |cb| cb.call(idx) }
      end

      # Updates the database configuration
      assert_equal expected_primary_database, ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "primary").database
      assert_equal expected_replica_database, ActiveRecord::Base.configurations.configs_for(env_name: "arunit", name: "replica", include_hidden: true).database
    ensure
      ActiveRecord::Base.configurations = prev_configs
      ActiveRecord::Base.establish_connection(:arunit)
      ENV["RAILS_ENV"] = previous_env
    end
  end
end
