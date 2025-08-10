# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module ReferentialIntegrity # :nodoc:
        def disable_referential_integrity # :nodoc:
          original_exception = nil

          if replication_role = @raw_connection&.parameter_status("session_replication_role")
            raise "replication role = '#{replication_role}'"
          end

          unless defined?(@session_replication_role)
            @session_replication_role = select_value("SELECT setting FROM pg_settings WHERE name = 'session_replication_role'", "SCHEMA")
          end

          begin
            if @session_replication_role
              execute("SET session_replication_role = 'replica'", "SCHEMA")
            else
              transaction(requires_new: true) do
                execute_batch(tables.collect { |name| "ALTER TABLE #{quote_table_name(name)} DISABLE TRIGGER ALL" })
              end
            end
          rescue ActiveRecord::ActiveRecordError => e
            original_exception = e
          end

          begin
            yield
          rescue ActiveRecord::InvalidForeignKey => e
            warn <<-WARNING
WARNING: Rails was not able to disable referential integrity.

This is most likely caused due to missing permissions.
Rails needs superuser privileges to disable referential integrity.

    cause: #{original_exception&.message}

            WARNING
            raise e
          end

          begin
            if @session_replication_role
              execute("SET session_replication_role = #{quote(@session_replication_role)}", "SCHEMA")
            else
              transaction(requires_new: true) do
                execute_batch(tables.collect { |name| "ALTER TABLE #{quote_table_name(name)} ENABLE TRIGGER ALL" })
              end
            end
          rescue ActiveRecord::ActiveRecordError
          end
        end

        def check_all_foreign_keys_valid! # :nodoc:
          sql = <<~SQL
            do $$
              declare r record;
            BEGIN
            FOR r IN (
              SELECT FORMAT(
                'UPDATE pg_catalog.pg_constraint SET convalidated=false WHERE conname = ''%1$I'' AND connamespace::regnamespace = ''%2$I''::regnamespace; ALTER TABLE %2$I.%3$I VALIDATE CONSTRAINT %1$I;',
                constraint_name,
                table_schema,
                table_name
              ) AS constraint_check
              FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY'
            )
              LOOP
                EXECUTE (r.constraint_check);
              END LOOP;
            END;
            $$;
          SQL

          transaction(requires_new: true) do
            execute(sql)
          end
        end
      end
    end
  end
end
