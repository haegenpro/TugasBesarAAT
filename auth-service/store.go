package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"time"

	"authservice/internal/platform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
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
	db          *sql.DB
	accessTTL   time.Duration
	refreshTTL  time.Duration
	fieldDB     *sql.DB
	priorityKey string
}

func openStore(path string, accessTTL, refreshTTL time.Duration) (*sessionStore, error) {
	cfg, err := pgx.ParseConfig(path)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL configuration")
	}
	cfg.Tracer = platform.SQLTracer{}
	cfg.RuntimeParams["statement_timeout"] = strconv.FormatInt(platform.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second).Milliseconds(), 10)
	cfg.RuntimeParams["lock_timeout"] = strconv.FormatInt(platform.Duration("DB_LOCK_TIMEOUT", 2*time.Second).Milliseconds(), 10)
	db := stdlib.OpenDB(*cfg)
	configurePool(db)
	store := &sessionStore{db: db, accessTTL: accessTTL, refreshTTL: refreshTTL}
	if os.Getenv("AUTH_PRIORITY_ENABLED") == "true" {
		key := os.Getenv("AUTH_PRIORITY_KEY")
		total := platform.Int("DB_MAX_OPEN_CONNS", 16)
		reserved, e := strconv.Atoi(os.Getenv("AUTH_DB_FIELD_RESERVED"))
		if e != nil || reserved < 1 || reserved >= total || len(key) < 32 {
			db.Close()
			return nil, errors.New("valid AUTH_PRIORITY_KEY and AUTH_DB_FIELD_RESERVED below DB_MAX_OPEN_CONNS required")
		}
		db.SetMaxOpenConns(total - reserved)
		db.SetMaxIdleConns(min(8, total-reserved))
		store.fieldDB = stdlib.OpenDB(*cfg.Copy())
		store.fieldDB.SetMaxOpenConns(reserved)
		store.fieldDB.SetMaxIdleConns(min(8, reserved))
		store.fieldDB.SetConnMaxLifetime(30 * time.Minute)
		store.priorityKey = key
	}
	return store, nil
}

func (s *sessionStore) Close() {
	s.db.Close()
	if s.fieldDB != nil {
		s.fieldDB.Close()
	}
}
func (s *sessionStore) tokenDB(token, kind string) *sql.DB {
	if s.fieldDB != nil && platform.FieldPriorityToken(token, kind, s.priorityKey, time.Now()) {
		return s.fieldDB
	}
	return s.db
}
func (s *sessionStore) signed(token, kind, role string, expiry int64) string {
	if s.priorityKey == "" {
		return token
	}
	return platform.SignPriorityToken(token, kind, role, s.priorityKey, expiry)
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
	access = s.signed(access, "access", identity.Role, now.Add(s.accessTTL).Unix())
	refresh = s.signed(refresh, "refresh", identity.Role, now.Add(s.refreshTTL).Unix())
	db := s.db
	if identity.Role == "field" && s.fieldDB != nil {
		db = s.fieldDB
	}
	_, err = db.ExecContext(ctx, `INSERT INTO sessions
		(session_id, client_id, role, access_hash, access_expires_at, refresh_hash, refresh_expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, sessionID, identity.ID, identity.Role,
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
	err := s.tokenDB(access, "access").QueryRowContext(ctx, `SELECT client_id, role FROM sessions WHERE access_hash = $1 AND access_expires_at > $2`, tokenHash(access), time.Now().Unix()).Scan(&p.ClientID, &p.Role)
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
	db := s.tokenDB(oldRefresh, "refresh")
	err := db.QueryRowContext(ctx, `SELECT client_id, role FROM sessions WHERE refresh_hash = $1 AND refresh_expires_at > $2`, oldHash, now.Unix()).Scan(&p.ClientID, &p.Role)
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
	access = s.signed(access, "access", p.Role, now.Add(s.accessTTL).Unix())
	refresh = s.signed(refresh, "refresh", p.Role, now.Add(s.refreshTTL).Unix())
	result, err := db.ExecContext(ctx, `UPDATE sessions SET access_hash = $1, access_expires_at = $2, refresh_hash = $3, refresh_expires_at = $4 WHERE refresh_hash = $5 AND refresh_expires_at > $6`,
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
	result, err := s.tokenDB(refresh, "refresh").ExecContext(ctx, `DELETE FROM sessions WHERE refresh_hash = $1`, tokenHash(refresh))
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
