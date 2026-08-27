# frozen_string_literal: true

module ARTest
  module RactorConnectionProxyTestMode
    ENV_KEY = "AR_RACTOR_PROXY"

    class << self
      attr_reader :target_connection_handler, :baseline_configurations

      def enabled?
        ENV[ENV_KEY] == "1"
      end

      def install!
        return unless enabled?
        return if @target_connection_handler

        @target_connection_handler = ActiveRecord::Base.connection_handler
        @baseline_configurations = ActiveRecord::Base.configurations
        ActiveRecord::ConnectionAdapters::RactorConnectionProxy.singleton_class.prepend(TargetConnectionHandler)
        ActiveRecord::ConnectionAdapters::RactorConnectionHandler.prepend(FixtureConnectionHandler)
        ActiveRecord::ConnectionAdapters::RactorConnectionPool.prepend(FixtureConnectionPool)
        ActiveRecord::TestCase.prepend(TestCaseCleanup)
        @target_connection_handler.clear_active_connections!(:all)
        ActiveRecord::Base.connection_handler = ActiveRecord::ConnectionAdapters::RactorConnectionHandler.instance
      end

      def fixture_pins
        ActiveSupport::IsolatedExecutionState[:active_record_ractor_proxy_fixture_pins] ||= {}
      end

      def target_connection(connection)
        return connection unless connection.is_a?(ActiveRecord::ConnectionAdapters::RactorConnectionProxy)

        ActiveRecord::ConnectionAdapters::RactorConnectionProxy.connections.fetch(connection.connection_token)
      end

      def target_connection_pool(pool)
        return pool unless pool.is_a?(ActiveRecord::ConnectionAdapters::RactorConnectionPool)

        target_connection_handler.retrieve_connection_pool(
          pool.connection_descriptor.name,
          role: pool.role,
          shard: pool.shard,
          strict: true,
        )
      end

      def target_handler(handler)
        if handler.is_a?(ActiveRecord::ConnectionAdapters::RactorConnectionHandler)
          target_connection_handler
        else
          handler
        end
      end

      def restore_baseline_connections!
        base_pool = target_connection_handler.retrieve_connection_pool("ActiveRecord::Base")
        secondary_pool = target_connection_handler.retrieve_connection_pool("ARUnit2Model")
        return if base_pool && secondary_pool

        ActiveRecord::Base.connection_handler = target_connection_handler
        ActiveRecord::Base.configurations = baseline_configurations
        ActiveRecord::Base.establish_connection(:arunit) unless base_pool
        ARUnit2Model.establish_connection(:arunit2) unless secondary_pool
      ensure
        ActiveRecord::Base.connection_handler = ActiveRecord::ConnectionAdapters::RactorConnectionHandler.instance
      end
    end

    module TargetConnectionHandler
      def connection_handler
        ARTest::RactorConnectionProxyTestMode.target_connection_handler
      end
    end

    # Transactional fixtures need the concrete handler's pool-manager topology,
    # but pin their connections through RactorConnectionPool so the proxy's
    # transaction manager sees the fixture transaction.
    module FixtureConnectionHandler
      def connection_pool_names
        ARTest::RactorConnectionProxyTestMode.target_connection_handler.connection_pool_names
      end

      private
        def connection_name_to_pool_manager
          ARTest::RactorConnectionProxyTestMode.target_connection_handler.send(:connection_name_to_pool_manager)
        end
    end

    module FixtureConnectionPool
      def pin_connection!(lock_thread)
        target_pool = ARTest::RactorConnectionProxyTestMode.target_connection_pool(self)
        if connection = active_connection?
          if connection.connected?
            target_connection = ARTest::RactorConnectionProxyTestMode.target_connection(connection)
            remove(connection) unless target_connection.pool.equal?(target_pool)
          else
            release_connection
          end
        end

        pins = ARTest::RactorConnectionProxyTestMode.fixture_pins
        pin = pins[object_id] ||= { connection: lease_connection, depth: 0 }
        pin[:depth] += 1
        pin[:connection].lock_thread = ActiveSupport::IsolatedExecutionState.context if lock_thread
        pin[:connection].begin_transaction(joinable: false, _lazy: false)
      end

      def unpin_connection!
        pins = ARTest::RactorConnectionProxyTestMode.fixture_pins
        pin = pins.fetch(object_id)
        connection = pin[:connection]
        clean = connection.connected? && connection.transaction_open?

        if clean
          connection.rollback_transaction
        elsif connection.connected?
          connection.reset!
        end

        pin[:depth] -= 1
        if pin[:depth].zero?
          connection.lock_thread = nil
          release_connection
          pins.delete(object_id)
        end

        clean
      end
    end

    module InstallAfterTestFilesLoad
      def self.minitest_plugin_init(_options)
        ARTest::RactorConnectionProxyTestMode.install!
      end
    end

    module TestCaseCleanup
      def after_teardown
        super
      ensure
        ARTest::RactorConnectionProxyTestMode.restore_baseline_connections!
      end

      def check_connection_leaks(connection_pools = nil)
        ActiveRecord::ConnectionAdapters::RactorConnectionHandler.instance.clear_active_connections!(:all)
        ActiveRecord::ConnectionAdapters::RactorConnectionProxy.checkin_all_connections
        connection_pools ||= ARTest::RactorConnectionProxyTestMode.target_connection_handler.connection_pool_list
        super(connection_pools)
      end

      private
        # Fixture insertion uses the block-taking disable_referential_integrity
        # API, so leave that test scaffolding on the concrete connection.
        def load_fixtures(config)
          proxy_handler = ActiveRecord::Base.connection_handler
          ActiveRecord::Base.connection_handler = ARTest::RactorConnectionProxyTestMode.target_connection_handler
          super
        ensure
          ActiveRecord::Base.connection_handler = proxy_handler
        end
    end
  end
end

Minitest.register_plugin(ARTest::RactorConnectionProxyTestMode::InstallAfterTestFilesLoad) if
  ARTest::RactorConnectionProxyTestMode.enabled?
