CREATE TABLE sessions (
 session_id TEXT PRIMARY KEY, client_id TEXT NOT NULL, role TEXT NOT NULL,
 access_hash TEXT NOT NULL UNIQUE, access_expires_at BIGINT NOT NULL,
 refresh_hash TEXT NOT NULL UNIQUE, refresh_expires_at BIGINT NOT NULL
);
CREATE INDEX sessions_refresh_expiry ON sessions(refresh_expires_at);
