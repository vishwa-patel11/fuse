-- Spark SQL equivalent of legacy dbo.ts_est()
-- Returns current timestamp in Eastern Time (America/New_York)
CREATE OR REPLACE TEMPORARY FUNCTION ts_est()
RETURNS TIMESTAMP
RETURN current_timestamp() AT TIME ZONE 'America/New_York';
