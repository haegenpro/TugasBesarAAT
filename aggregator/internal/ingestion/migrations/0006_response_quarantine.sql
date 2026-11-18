-- Outer HTTP responses can be larger than decoder/cache bounds. Preserve all
-- bytes in bounded chunks, exposed through the existing Ops-only payload route.
ALTER TABLE ingestion_failure_payloads
 ADD COLUMN chunked boolean NOT NULL DEFAULT false,
 ADD COLUMN size bigint NOT NULL DEFAULT 0 CHECK(size>=0);
UPDATE ingestion_failure_payloads SET size=octet_length(payload);
CREATE TABLE ingestion_failure_payload_chunks (
 namespace text COLLATE "C" NOT NULL,
 message_id text COLLATE "C" NOT NULL,
 ordinal integer NOT NULL CHECK(ordinal>=0),
 data bytea NOT NULL CHECK(octet_length(data)<=65536),
 PRIMARY KEY(namespace,message_id,ordinal),
 FOREIGN KEY(namespace,message_id) REFERENCES ingestion_failures(namespace,message_id)
);
