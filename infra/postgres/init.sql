-- Runs once, on an empty data volume, as the POSTGRES_USER superuser (postgres).
-- This file alone owns database creation.
CREATE DATABASE pixelmagic;
CREATE DATABASE temporal;
CREATE DATABASE temporal_visibility;

CREATE ROLE pm_reader LOGIN PASSWORD 'pm_reader';

\connect pixelmagic
GRANT CONNECT ON DATABASE pixelmagic TO pm_reader;
GRANT USAGE ON SCHEMA public TO pm_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO pm_reader;

\connect temporal_visibility
GRANT CONNECT ON DATABASE temporal_visibility TO pm_reader;
GRANT USAGE ON SCHEMA public TO pm_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO pm_reader;
