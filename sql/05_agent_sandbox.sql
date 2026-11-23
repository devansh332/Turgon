-- Agent-generated SQL runs as this role (db.read_only and schema_agent.verify use SET LOCAL ROLE).
-- It can read bronze and create throwaway tables in silver_proposed, nothing else: the parser checks
-- in sql_guard are the first line of defence, these grants are the one that cannot be talked around.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'medallion_agent') THEN
        CREATE ROLE medallion_agent NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS silver_proposed;
REVOKE ALL ON SCHEMA public FROM medallion_agent;
GRANT USAGE ON SCHEMA bronze TO medallion_agent;
GRANT SELECT ON bronze.tickets_raw TO medallion_agent;
GRANT USAGE, CREATE ON SCHEMA silver_proposed TO medallion_agent;
