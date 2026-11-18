-- Failed health/backoff attempts must not consume a saved upstream receipt.
ALTER TABLE source_poll_state ADD COLUMN dispatch_generation bigint NOT NULL DEFAULT 0 CHECK(dispatch_generation>=0);
