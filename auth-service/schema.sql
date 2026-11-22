CREATE TABLE sessions (
 session_id TEXT PRIMARY KEY, client_id TEXT NOT NULL, role TEXT NOT NULL,
 access_hash TEXT NOT NULL UNIQUE, access_expires_at BIGINT NOT NULL,
 refresh_hash TEXT NOT NULL UNIQUE, refresh_expires_at BIGINT NOT NULL
);
CREATE INDEX sessions_refresh_expiry ON sessions(refresh_expires_at);
-- Refresh grace window: the previous refresh token and the per-rotation salt from which the
-- current pair was derived, so re-presenting the just-rotated token returns the same pair.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS previous_refresh_hash TEXT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS rotated_at BIGINT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS rotation_salt TEXT;
CREATE INDEX IF NOT EXISTS sessions_previous_refresh ON sessions(previous_refresh_hash);
