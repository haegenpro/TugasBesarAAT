-- A dispatch intent never expires. Lease expiry cannot prove that its sender
-- has stopped between the database decision and the actual socket write.
-- One bounded response per source/lane allows a new owner to reuse a receipt.
CREATE TABLE acquisition_dispatch (
 scope text NOT NULL REFERENCES canonical_ownership(scope),
 lane text NOT NULL CHECK (length(lane) BETWEEN 1 AND 256),
 request_key text NOT NULL,
 fingerprint text NOT NULL,
 dispatch_id uuid NOT NULL UNIQUE,
 epoch bigint NOT NULL CHECK (epoch>0),
 holder text NOT NULL,
 state text NOT NULL CHECK (state IN ('pending','complete','closed')),
 streaming boolean NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 completed_at timestamptz,
 status integer,
 headers jsonb,
 body bytea,
 PRIMARY KEY(scope,lane),
 CHECK (state<>'complete' OR (NOT streaming AND status BETWEEN 100 AND 599 AND headers IS NOT NULL AND body IS NOT NULL)),
 CHECK (state<>'closed' OR streaming)
);
