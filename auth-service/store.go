package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	_ "modernc.org/sqlite"
)

var errInvalidToken = errors.New("invalid token")

type tokenPair struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
	Role             string `json:"role"`
	ClientID         string `json:"client_id"`
	Scope            string `json:"scope"`
}

type principal struct {
	ClientID string `json:"client_id"`
	Role     string `json:"role"`
	Scope    string `json:"scope"`
}

type sessionStore struct {
	db         *sql.DB
	accessTTL  time.Duration
	refreshTTL time.Duration
}

func openStore(path string, accessTTL, refreshTTL time.Duration) (*sessionStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			client_id TEXT NOT NULL,
			role TEXT NOT NULL,
			access_hash TEXT NOT NULL UNIQUE,
			access_expires_at INTEGER NOT NULL,
			refresh_hash TEXT NOT NULL UNIQUE,
			refresh_expires_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS sessions_refresh_expiry ON sessions(refresh_expires_at)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &sessionStore{db: db, accessTTL: accessTTL, refreshTTL: refreshTTL}, nil
}

func newToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s *sessionStore) issue(ctx context.Context, identity identity) (tokenPair, error) {
	access, err := newToken()
	if err != nil {
		return tokenPair{}, err
	}
	refresh, err := newToken()
	if err != nil {
		return tokenPair{}, err
	}
	sessionID, err := newToken()
	if err != nil {
		return tokenPair{}, err
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO sessions
		(session_id, client_id, role, access_hash, access_expires_at, refresh_hash, refresh_expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, sessionID, identity.ID, identity.Role,
		tokenHash(access), now.Add(s.accessTTL).Unix(), tokenHash(refresh), now.Add(s.refreshTTL).Unix())
	if err != nil {
		return tokenPair{}, err
	}
	return s.pair(identity.ID, identity.Role, access, refresh), nil
}

func (s *sessionStore) pair(clientID, role, access, refresh string) tokenPair {
	return tokenPair{AccessToken: access, RefreshToken: refresh, TokenType: "Bearer", ExpiresIn: int64(s.accessTTL.Seconds()), RefreshExpiresIn: int64(s.refreshTTL.Seconds()), Role: role, ClientID: clientID, Scope: scopeForRole(role)}
}

func scopeForRole(role string) string {
	if role == "internal" || role == "field" {
		return "hazards:raw"
	}
	return "hazards:summary"
}

func (s *sessionStore) verify(ctx context.Context, access string) (principal, error) {
	if access == "" {
		return principal{}, errInvalidToken
	}
	var p principal
	err := s.db.QueryRowContext(ctx, `SELECT client_id, role FROM sessions WHERE access_hash = ? AND access_expires_at > ?`, tokenHash(access), time.Now().Unix()).Scan(&p.ClientID, &p.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return principal{}, errInvalidToken
	}
	p.Scope = scopeForRole(p.Role)
	return p, err
}

func (s *sessionStore) rotate(ctx context.Context, oldRefresh string) (tokenPair, error) {
	if oldRefresh == "" {
		return tokenPair{}, errInvalidToken
	}
	var p principal
	oldHash := tokenHash(oldRefresh)
	now := time.Now().UTC()
	err := s.db.QueryRowContext(ctx, `SELECT client_id, role FROM sessions WHERE refresh_hash = ? AND refresh_expires_at > ?`, oldHash, now.Unix()).Scan(&p.ClientID, &p.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return tokenPair{}, errInvalidToken
	}
	if err != nil {
		return tokenPair{}, err
	}
	access, err := newToken()
	if err != nil {
		return tokenPair{}, err
	}
	refresh, err := newToken()
	if err != nil {
		return tokenPair{}, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sessions SET access_hash = ?, access_expires_at = ?, refresh_hash = ?, refresh_expires_at = ? WHERE refresh_hash = ? AND refresh_expires_at > ?`,
		tokenHash(access), now.Add(s.accessTTL).Unix(), tokenHash(refresh), now.Add(s.refreshTTL).Unix(), oldHash, now.Unix())
	if err != nil {
		return tokenPair{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return tokenPair{}, err
	}
	if count != 1 {
		return tokenPair{}, errInvalidToken
	}
	return s.pair(p.ClientID, p.Role, access, refresh), nil
}

func (s *sessionStore) logout(ctx context.Context, refresh string) error {
	if refresh == "" {
		return errInvalidToken
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE refresh_hash = ?`, tokenHash(refresh))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errInvalidToken
	}
	return nil
}
