-- Create the app_logs table for storing application logs

CREATE TABLE IF NOT EXISTS app_logs (
    timestamp DateTime64(3),
    service_id String,
    level String,
    message String,
    trace_id String,
    user_id String,
    ingested_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (service_id, timestamp)
SETTINGS index_granularity = 8192;

-- Optional: Create a materialized view for aggregated stats
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_by_service_hourly
ENGINE = SummingMergeTree()
ORDER BY (service_id, hour, level)
AS SELECT
    service_id,
    toStartOfHour(timestamp) AS hour,
    level,
    count() AS log_count
FROM app_logs
GROUP BY service_id, hour, level;
