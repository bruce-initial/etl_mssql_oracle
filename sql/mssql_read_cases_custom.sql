-- Custom query with joins using ODBC connection
-- Note: Complex joins should be done in DuckDB after importing tables separately
-- This is a simplified version that reads from CASES table with filters

SELECT
    id
  , name
FROM tempCase