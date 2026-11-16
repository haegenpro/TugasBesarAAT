-- Legacy failures stay with the Aggregator owner. Large response bodies are
-- streamed into bounded chunks, never silently kept as a truncated prefix.
CREATE TABLE failed_ingestion_payloads (
 payload_id uuid PRIMARY KEY, size bigint NOT NULL DEFAULT 0,
 sha256 text NOT NULL DEFAULT '', content_type text NOT NULL DEFAULT ''
);
CREATE TABLE failed_ingestion_payload_chunks (
 payload_id uuid NOT NULL REFERENCES failed_ingestion_payloads ON DELETE CASCADE,
 ordinal integer NOT NULL, data bytea NOT NULL CHECK(octet_length(data)<=65536),
 PRIMARY KEY(payload_id,ordinal)
);
ALTER TABLE failed_ingestions ADD COLUMN payload_id uuid REFERENCES failed_ingestion_payloads;
