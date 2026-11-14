// Package pagecursor signs service-issued live traversal positions. It does not
// claim retained snapshot membership or accept caller-created unsigned offsets.
package pagecursor

import (
	"aggregator/internal/canonical"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Position struct {
	Version   int       `json:"v"`
	Endpoint  string    `json:"endpoint"`
	Scope     string    `json:"scope"`
	Filters   string    `json:"filters"`
	Time      time.Time `json:"time"`
	ID        string    `json:"id"`
	Namespace string    `json:"namespace"`
	Units     []string  `json:"units"`
	Snapshot  *string   `json:"snapshot_id"`
}
type Signer struct{ Key []byte }

func (s Signer) Valid() bool { return len(s.Key) >= 32 }
func (s Signer) Encode(p Position) (string, error) {
	if !s.Valid() {
		return "", canonical.Unavailable()
	}
	p.Version = 1
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	h := hmac.New(sha256.New, s.Key)
	_, _ = h.Write(b)
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}
func (s Signer) Decode(token, endpoint, scope, filters string) (Position, error) {
	return s.DecodeSnapshot(token, endpoint, scope, filters, nil)
}
func (s Signer) DecodeSnapshot(token, endpoint, scope, filters string, snapshot *string) (Position, error) {
	var p Position
	bad := fmt.Errorf("invalid cursor")
	if !s.Valid() {
		return p, canonical.Unavailable()
	}
	if len(token) > 16384 {
		return p, bad
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return p, bad
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return p, bad
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return p, bad
	}
	h := hmac.New(sha256.New, s.Key)
	_, _ = h.Write(b)
	if !hmac.Equal(sig, h.Sum(nil)) {
		return p, bad
	}
	if err = canonical.Decode(bytes.NewReader(b), &p); err != nil || p.Version != 1 || p.Endpoint != endpoint || p.Scope != scope || p.Filters != filters || (p.Snapshot == nil) != (snapshot == nil) || (p.Snapshot != nil && *p.Snapshot != *snapshot) {
		return Position{}, bad
	}
	return p, nil
}
