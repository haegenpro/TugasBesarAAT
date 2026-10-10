// Package history owns this source's durable replay archive. Other services use HTTP.
package history

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
)

type Archive struct{ DB *sql.DB }
type Record struct {
	Entity  string
	ID      string
	Payload any
}
type Item struct {
	Cursor  int64           `json:"cursor"`
	Payload json.RawMessage `json:"payload"`
}
type Page struct {
	Data       []Item `json:"data"`
	NextCursor int64  `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

func Open(path string) (*Archive, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `CREATE TABLE IF NOT EXISTS history (sequence INTEGER PRIMARY KEY AUTOINCREMENT, entity TEXT NOT NULL, record_id TEXT NOT NULL, payload TEXT NOT NULL)`, `CREATE INDEX IF NOT EXISTS history_entity ON history(entity,sequence)`, `CREATE INDEX IF NOT EXISTS history_record ON history(entity,record_id,sequence)`} {
		if _, err = db.Exec(q); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Archive{db}, nil
}

// Append commits an observation and any dependent records before live exposure.
func (a *Archive) Append(records ...Record) error {
	tx, err := a.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, r := range records {
		b, err := json.Marshal(r.Payload)
		if err != nil {
			return err
		}
		var previous string
		err = tx.QueryRow(`SELECT payload FROM history WHERE entity=? AND record_id=? ORDER BY sequence DESC LIMIT 1`, r.Entity, r.ID).Scan(&previous)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if previous == string(b) {
			continue
		}
		if _, err = tx.Exec(`INSERT INTO history(entity,record_id,payload) VALUES (?,?,?)`, r.Entity, r.ID, string(b)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (a *Archive) Read(entity string, after int64, limit int) (Page, error) {
	p := Page{Data: []Item{}, NextCursor: after}
	if after < 0 || limit < 1 || limit > 1000 {
		return p, fmt.Errorf("invalid cursor or limit")
	}
	rows, err := a.DB.Query(`SELECT sequence,payload FROM history WHERE entity=? AND sequence>? ORDER BY sequence LIMIT ?`, entity, after, limit+1)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var i Item
		var b string
		if err = rows.Scan(&i.Cursor, &b); err != nil {
			return p, err
		}
		i.Payload = json.RawMessage(b)
		if len(p.Data) == limit {
			p.HasMore = true
			break
		}
		p.Data = append(p.Data, i)
		p.NextCursor = i.Cursor
	}
	return p, rows.Err()
}

// Latest returns one durable version per ID, including records evicted from the
// live window. Schema changes must publish revisions for these records too.
func (a *Archive) Latest(entity string) ([]json.RawMessage, error) {
	rows, err := a.DB.Query(`SELECT payload FROM history WHERE sequence IN (SELECT MAX(sequence) FROM history WHERE entity=? GROUP BY record_id) ORDER BY sequence`, entity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, json.RawMessage(raw))
	}
	return out, rows.Err()
}

func (a *Archive) Head() (int64, error) {
	var head int64
	err := a.DB.QueryRow(`SELECT COALESCE(MAX(sequence),0) FROM history`).Scan(&head)
	return head, err
}
