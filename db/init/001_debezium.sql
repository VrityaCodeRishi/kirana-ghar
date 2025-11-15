-- Init script to prepare logical decoding for Debezium
-- Ensure the replication user (created by Bitnami) has read access
GRANT CONNECT ON DATABASE grocerydb TO repl_user;
GRANT USAGE ON SCHEMA public TO repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_user;

-- Note: The Debezium publication "dbz_publication" is created by the
-- application after tables are created, in backend startup. This avoids
-- failures here when the tables don't exist yet.
