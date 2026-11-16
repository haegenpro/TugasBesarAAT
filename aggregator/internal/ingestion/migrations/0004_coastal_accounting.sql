CREATE TABLE coastal_source_status (
 namespace text COLLATE "C" PRIMARY KEY, snapshot jsonb NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE coastal_warning_expected (
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 notification_id text COLLATE "C" NOT NULL UNIQUE,
 namespace text COLLATE "C" NOT NULL, observation_id text NOT NULL,
 operation_id text NOT NULL, payload_sha256 text NOT NULL,
 first_received_at timestamptz NOT NULL, observed_at text NOT NULL,
 rejected_at timestamptz, rejection_reason text
);
CREATE INDEX coastal_warning_expected_window ON coastal_warning_expected(namespace,first_received_at,sequence);
CREATE TABLE coastal_warning_coverage (
 namespace text COLLATE "C" PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
