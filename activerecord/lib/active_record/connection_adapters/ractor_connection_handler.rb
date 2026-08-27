# frozen_string_literal: true

# :markup: markdown

module ActiveRecord
  module ConnectionAdapters
    class RactorConnectionHandler # :nodoc:
      INSTANCE = new.freeze

      class << self
        def instance = INSTANCE

        def config_spec(config)
          spec = case config
          when ActiveRecord::DatabaseConfigurations::UrlConfig
            [:url, config.env_name, config.name, config.url, config.configuration_hash]
          when ActiveRecord::DatabaseConfigurations::HashConfig
            [:hash, config.env_name, config.name, config.configuration_hash]
          else
            [:raw, config]
          end
          RactorConnectionProxy.shareable_copy(spec)
        end

        def config_from_spec(spec)
          type, *values = spec
          case type
          when :url
            ActiveRecord::DatabaseConfigurations::UrlConfig.new(*values)
          when :hash
            ActiveRecord::DatabaseConfigurations::HashConfig.new(*values)
          when :raw
            values.first
          end
        end
      end

      def connection_pool_list(role = nil)
        RactorConnectionProxy.main_pool_specs(role).map { |pool_spec| RactorConnectionPool.new(pool_spec) }
      end
      alias :connection_pools :connection_pool_list

      def each_connection_pool(role = nil, &block)
        return enum_for(__method__, role) unless block_given?

        connection_pool_list(role).each(&block)
      end

      def retrieve_connection(connection_name, role: ActiveRecord::Base.current_role, shard: ActiveRecord::Base.current_shard)
        retrieve_connection_pool(connection_name, role: role, shard: shard, strict: true).lease_connection
      end

      def retrieve_connection_pool(connection_name, role: ActiveRecord::Base.current_role, shard: ActiveRecord::Base.current_shard, strict: false)
        pool_spec = RactorConnectionProxy.main_pool_spec(connection_name.to_s, role, shard, strict)
        pool_spec && RactorConnectionPool.new(pool_spec)
      end

      def connected?(connection_name, role: ActiveRecord::Base.current_role, shard: ActiveRecord::Base.current_shard)
        pool = retrieve_connection_pool(connection_name, role: role, shard: shard)
        pool && pool.connected?
      end

      def active_connections?(role = nil)
        each_connection_pool(role).any?(&:active_connection?)
      end

      def clear_active_connections!(role = nil)
        each_connection_pool(role).each do |pool|
          pool.release_connection
          pool.disable_query_cache!
        end
      end

      def clear_reloadable_connections!(role = nil)
        clear_active_connections!(role)
      end

      def clear_all_connections!(role = nil)
        each_connection_pool(role).each(&:disconnect!)
      end

      def flush_idle_connections!(role = nil)
        each_connection_pool(role).each(&:flush!)
      end

      def remove_connection_pool(connection_name, role: ActiveRecord::Base.current_role, shard: ActiveRecord::Base.current_shard)
        if pool = retrieve_connection_pool(connection_name, role: role, shard: shard)
          pool.release_connection
        end
        RactorConnectionProxy.remove_connection_pool(connection_name, role, shard)
      end

      def establish_connection(config, owner_name: Base, role: Base.current_role, shard: Base.current_shard, clobber: false)
        connection_owner_name = (owner_name.respond_to?(:name) ? owner_name.name : owner_name).to_s
        db_config_spec = self.class.config_spec(config)
        connection_role = role
        connection_shard = shard
        clobber_existing = clobber

        pool_spec = ActiveSupport::Ractors.on_main do
          db_config = RactorConnectionHandler.config_from_spec(db_config_spec)
          pool = ActiveRecord::Base.connection_handler.establish_connection(
            db_config,
            owner_name: connection_owner_name,
            role: connection_role,
            shard: connection_shard,
            clobber: clobber_existing,
          )
          RactorConnectionPool.spec_for(pool)
        end

        RactorConnectionPool.new(pool_spec)
      end
    end
  end
end
