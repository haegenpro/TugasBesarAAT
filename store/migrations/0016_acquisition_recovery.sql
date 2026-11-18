-- Bounded automatic recovery for read-only source requests. Original intent
-- records/checksums remain intact; recovery supersedes a nonce, never a cursor.
ALTER TABLE acquisition_dispatch
 ADD COLUMN attempt_started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN retry_after timestamptz NOT NULL DEFAULT (clock_timestamp()+interval '10 seconds'),
 ADD COLUMN stream_open boolean NOT NULL DEFAULT false,
 ADD COLUMN interrupted boolean NOT NULL DEFAULT false,
 ADD COLUMN recovery_count bigint NOT NULL DEFAULT 0 CHECK(recovery_count>=0),
 ADD COLUMN previous_dispatch_id uuid,
 ADD COLUMN last_recovery_at timestamptz,
 ADD COLUMN last_failure text NOT NULL DEFAULT '';
UPDATE acquisition_dispatch SET attempt_started_at=started_at,retry_after=started_at+interval '10 seconds';
ALTER TABLE acquisition_dispatch DROP CONSTRAINT acquisition_dispatch_state_check;
ALTER TABLE acquisition_dispatch ADD CONSTRAINT acquisition_dispatch_state_check
 CHECK(state IN ('pending','complete','closed','abandoned'));
ALTER TABLE acquisition_dispatch ADD CONSTRAINT acquisition_dispatch_open_check
 CHECK(NOT stream_open OR (streaming AND state='pending'));
