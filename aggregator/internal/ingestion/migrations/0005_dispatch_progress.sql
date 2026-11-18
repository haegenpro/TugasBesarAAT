-- Empty coastal pages must also advance the durable request generation;
-- position/cursor alone can remain unchanged when a successful page is empty.
ALTER TABLE source_progress ADD COLUMN dispatch_generation bigint NOT NULL DEFAULT 0 CHECK(dispatch_generation>=0);
-- Validators on other replicas read station metadata from this owned cache.
-- Only the source owner contacts the sensor's stations endpoint.
CREATE TABLE coastal_station_cache (
 namespace text PRIMARY KEY,
 payload jsonb NOT NULL,
 fetched_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
