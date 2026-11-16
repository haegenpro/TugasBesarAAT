-- Keep acquisition provenance immutable across publish/cursor crash windows.
-- Rows below the confirmed cursor are retired in the same fenced transaction.
CREATE TABLE source_envelopes (
 source text NOT NULL, namespace text COLLATE "C" NOT NULL,
 position bigint NOT NULL CHECK(position>=0), envelope jsonb NOT NULL,
 PRIMARY KEY(source,namespace,position)
);
