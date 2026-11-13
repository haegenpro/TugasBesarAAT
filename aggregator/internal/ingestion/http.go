package ingestion

import (
	"aggregator/internal/canonical"
	"aggregator/internal/pagecursor"
	"aggregator/internal/rawinput"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type HTTP struct {
	Store            *Store
	ServiceToken     string
	Signer           pagecursor.Signer
	AuthURL, AuthKey string
	Client           *http.Client
	LegacyURL        string
}
type FailureItem struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int64     `json:"attempts"`
	Status     string    `json:"status"`
}

func (h HTTP) Register(r gin.IRoutes) {
	r.GET("/internal/ingestion/payloads/:id", h.payload)
	r.GET("/internal/ingestion/failures", h.failures)
	r.GET("/internal/ingestion/failures/:id/payload", h.failurePayload)
	r.POST("/internal/ingestion/replays", h.replay)
	r.GET("/internal/ingestion/replays/:id", h.replayStatus)
}
func (h HTTP) payload(c *gin.Context) {
	if !rawinput.Authorize(c, h.ServiceToken) {
		return
	}
	h.stream(c, c.Param("id"))
}
func (h HTTP) stream(c *gin.Context, id string) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	r, err := h.Store.Open(ctx, id)
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	defer r.Close()
	var size int64
	var sum string
	if err = h.Store.DB.Pool.QueryRow(ctx, `SELECT size,sha256 FROM ingestion_captures WHERE payload_id=$1`, id).Scan(&size, &sum); err != nil {
		rawinput.Error(c, err)
		return
	}
	c.Header("Content-Type", "application/octet-stream")
	c.Header("X-Payload-SHA256", sum)
	c.Header("Content-Length", strconv.FormatInt(size, 10))
	_, _ = io.CopyBuffer(c.Writer, r, make([]byte, 64<<10))
}
func (h HTTP) ops(c *gin.Context) (string, bool) {
	token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	if token == "" || token == c.GetHeader("Authorization") {
		rawinput.Error(c, canonical.Fail(401, "unauthorized", "Ops bearer required"))
		return "", false
	}
	if h.AuthURL == "" || h.AuthKey == "" || h.Client == nil {
		rawinput.Error(c, canonical.Unavailable())
		return "", false
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	b, _ := json.Marshal(map[string]string{"access_token": token})
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(h.AuthURL, "/")+"/internal/introspect", bytes.NewReader(b))
	if err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return "", false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", h.AuthKey)
	resp, err := h.Client.Do(req)
	if err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		status := resp.StatusCode
		if status != 401 && status != 403 {
			status = 503
		}
		rawinput.Error(c, canonical.Fail(status, "unauthorized", "Ops authentication unavailable or denied"))
		return "", false
	}
	var p struct {
		ID    string `json:"client_id"`
		Role  string `json:"role"`
		Scope string `json:"scope"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&p) != nil || p.ID == "" {
		rawinput.Error(c, canonical.Unavailable())
		return "", false
	}
	if p.Role != "internal" {
		rawinput.Error(c, canonical.Fail(403, "forbidden", "Internal Ops required"))
		return "", false
	}
	return p.ID, true
}
func (h HTTP) failures(c *gin.Context) {
	// Metadata service token is used only by Aggregator's audit proxy. Human Ops
	// uses introspection; neither authority grants canonical database access.
	scope := "audit"
	if c.GetHeader("Authorization") != "Bearer "+h.ServiceToken || h.ServiceToken == "" {
		var ok bool
		scope, ok = h.ops(c)
		if !ok {
			return
		}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	source := c.Query("source")
	if source != "" && source != "BMKG" && source != "SATELIT" && source != "PVMBG" && source != "SENSOR_PANTAI" {
		rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid source"))
		return
	}
	limit := 100
	if v := c.Query("limit"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 || n > 1000 {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid limit"))
			return
		}
		limit = n
	}
	status := c.Query("status")
	if status != "" && status != "retrying" && status != "parked" {
		rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid status"))
		return
	}
	filters := source + "|" + status
	pos := pagecursor.Position{}
	if v := c.Query("cursor"); v != "" {
		var err error
		pos, err = h.Signer.Decode(v, "unprocessed", scope, filters)
		if err != nil {
			rawinput.Error(c, canonical.Fail(400, "bad_request", "invalid cursor"))
			return
		}
	}
	rows, err := h.Store.DB.Pool.Query(ctx, `SELECT message_id,source,received_at,reason,status='retrying',attempts,status,namespace FROM ingestion_failures WHERE status<>'recovered' AND ($1='' OR source=$1) AND ($2='' OR status=$2) AND ($3::timestamptz IS NULL OR (received_at,message_id,namespace)>($3,$4,$5)) ORDER BY received_at,message_id COLLATE "C",namespace COLLATE "C" LIMIT $6`, source, status, nullableTime(pos.Time), pos.ID, pos.Namespace, limit+1)
	if err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	defer rows.Close()
	items := []FailureItem{}
	entries := []failureEntry{}
	var next *string
	var last pagecursor.Position
	for rows.Next() {
		var v FailureItem
		var ns string
		if err = rows.Scan(&v.MessageID, &v.Source, &v.ReceivedAt, &v.Reason, &v.Retryable, &v.Attempts, &v.Status, &ns); err != nil {
			rawinput.Error(c, err)
			return
		}
		if len(items) == limit {
			token, e := h.Signer.Encode(last)
			if e != nil {
				rawinput.Error(c, e)
				return
			}
			next = &token
			break
		}
		items = append(items, v)
		v.ReceivedAt = v.ReceivedAt.UTC()
		entries = append(entries, failureEntry{Item: v, Namespace: ns})
		last = pagecursor.Position{Endpoint: "unprocessed", Scope: scope, Filters: filters, Time: v.ReceivedAt, ID: v.MessageID, Namespace: ns}
	}
	if err = rows.Err(); err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	rows.Close()
	if err = h.Store.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_state WHERE namespace=$1)`, h.Store.Namespace); err != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	legacy, legacyErr := h.legacy(ctx, source, status, pos, limit+1)
	if legacyErr != nil {
		pos.Endpoint = "unprocessed"
		pos.Scope = scope
		pos.Filters = filters
		token, err := h.Signer.Encode(pos)
		if err != nil {
			rawinput.Error(c, err)
			return
		}
		c.JSON(200, gin.H{"items": []FailureItem{}, "next_cursor": token, "partial": true, "unavailable_units": []string{"legacy-ingestion"}})
		return
	}
	entries = append(entries, legacy...)
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Item.ReceivedAt.Equal(b.Item.ReceivedAt) {
			if a.Item.MessageID == b.Item.MessageID {
				return a.Namespace < b.Namespace
			}
			return a.Item.MessageID < b.Item.MessageID
		}
		return a.Item.ReceivedAt.Before(b.Item.ReceivedAt)
	})
	more := next != nil || len(entries) > limit
	if len(entries) > limit {
		entries = entries[:limit]
	}
	items = []FailureItem{}
	next = nil
	for _, v := range entries {
		items = append(items, v.Item)
	}
	if more && len(entries) > 0 {
		last := entries[len(entries)-1]
		p := pagecursor.Position{Endpoint: "unprocessed", Scope: scope, Filters: filters, Time: last.Item.ReceivedAt, ID: last.Item.MessageID, Namespace: last.Namespace}
		v, err := h.Signer.Encode(p)
		if err != nil {
			rawinput.Error(c, err)
			return
		}
		next = &v
	}
	c.JSON(200, gin.H{"items": items, "next_cursor": next, "partial": false, "unavailable_units": []string{}})
}

type failureEntry struct {
	Item      FailureItem `json:"item"`
	Namespace string      `json:"namespace"`
}

func (h HTTP) legacy(ctx context.Context, source, status string, p pagecursor.Position, limit int) ([]failureEntry, error) {
	if h.LegacyURL == "" || h.Client == nil {
		return nil, canonical.Unavailable()
	}
	q := url.Values{"source": {source}, "status": {status}, "limit": {strconv.Itoa(limit)}, "after_id": {p.ID}, "after_namespace": {p.Namespace}}
	if !p.Time.IsZero() {
		q.Set("after_time", p.Time.UTC().Format(time.RFC3339Nano))
	}
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(h.LegacyURL, "/")+"/internal/ingestion/legacy-failures?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.ServiceToken)
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, canonical.Unavailable()
	}
	var body struct {
		Items []failureEntry `json:"items"`
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&body)
	if err != nil || body.Items == nil {
		return nil, canonical.Unavailable()
	}
	return body.Items, nil
}
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
func (h HTTP) failurePayload(c *gin.Context) {
	if _, ok := h.ops(c); !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	var id string
	err := h.Store.DB.Pool.QueryRow(ctx, `SELECT c.payload_id FROM ingestion_failures f JOIN ingestion_captures c USING(namespace,position) WHERE f.namespace=$1 AND f.message_id=$2`, c.Query("namespace"), c.Param("id")).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		rawinput.Error(c, canonical.Fail(404, "not_found", "unknown failure"))
		return
	}
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	h.stream(c, id)
}

type replayRequest struct {
	RequestID string   `json:"request_id"`
	Source    string   `json:"source"`
	Namespace string   `json:"namespace"`
	Messages  []string `json:"message_ids"`
	Adapter   string   `json:"adapter_version"`
}

func (h HTTP) replay(c *gin.Context) {
	identity, ok := h.ops(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	var req replayRequest
	if err := canonical.Decode(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20), &req); err != nil || req.RequestID == "" || req.Source != "BMKG" || req.Namespace != h.Store.Namespace || len(req.Messages) < 1 || len(req.Messages) > 1000 || req.Adapter != "bmkg-capture-v1" {
		rawinput.Error(c, canonical.Fail(400, "invalid_request", "invalid replay scope or adapter"))
		return
	}
	fp, _ := canonical.Fingerprint(req)
	tx, err := h.Store.DB.Pool.Begin(ctx)
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	defer tx.Rollback(context.Background())
	if err = h.Store.Durability.Prepare(ctx, tx); err != nil {
		rawinput.Error(c, err)
		return
	}
	id := uuid.NewString()
	newID := id
	mb, _ := json.Marshal(req.Messages)
	var prior string
	err = tx.QueryRow(ctx, `INSERT INTO ingestion_replays(job_id,ops_identity,request_id,fingerprint,namespace,message_ids,adapter_version) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(ops_identity,request_id) DO UPDATE SET request_id=excluded.request_id RETURNING job_id,fingerprint`, id, identity, req.RequestID, fp, req.Namespace, mb, req.Adapter).Scan(&id, &prior)
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	if prior != fp {
		rawinput.Error(c, canonical.Fail(409, "identity_conflict", "replay identity reused"))
		return
	}
	if id != newID {
		if err = tx.Commit(ctx); err != nil {
			rawinput.Error(c, err)
			return
		}
		if err = h.Store.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_replays WHERE job_id=$1 AND fingerprint=$2)`, id, fp); err != nil {
			rawinput.Error(c, err)
			return
		}
		c.JSON(202, gin.H{"job_id": id, "status": "pending"})
		return
	}
	// Ops authorizes a retry; the live worker still uses its current acquisition
	// fence and the original sequence. No skipped offsets or revision bypass.
	for _, message := range req.Messages {
		tag, err := tx.Exec(ctx, `UPDATE ingestion_failures SET status='retrying',classification='transient',next_attempt_at=clock_timestamp() WHERE namespace=$1 AND message_id=$2 AND status<>'recovered'`, req.Namespace, message)
		if err != nil || tag.RowsAffected() != 1 {
			rawinput.Error(c, canonical.Fail(400, "invalid_request", "replay member unavailable or recovered"))
			return
		}
	}
	if err = tx.Commit(ctx); err != nil {
		rawinput.Error(c, err)
		return
	}
	if err = h.Store.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_replays WHERE job_id=$1 AND fingerprint=$2)`, id, fp); err != nil {
		rawinput.Error(c, err)
		return
	}
	c.JSON(202, gin.H{"job_id": id, "status": "pending"})
}
func (h HTTP) replayStatus(c *gin.Context) {
	identity, ok := h.ops(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
	defer cancel()
	var b []byte
	var ns string
	err := h.Store.DB.Pool.QueryRow(ctx, `SELECT namespace,message_ids FROM ingestion_replays WHERE job_id=$1 AND ops_identity=$2`, c.Param("id"), identity).Scan(&ns, &b)
	if err == pgx.ErrNoRows {
		rawinput.Error(c, canonical.Fail(404, "not_found", "unknown replay job"))
		return
	}
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	var ids []string
	if json.Unmarshal(b, &ids) != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	counts := map[string]int{"selected": len(ids), "recovered": 0, "retrying": 0, "parked": 0}
	rows, err := h.Store.DB.Pool.Query(ctx, `SELECT status,count(*) FROM ingestion_failures WHERE namespace=$1 AND message_id=ANY($2) GROUP BY status`, ns, ids)
	if err != nil {
		rawinput.Error(c, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if rows.Scan(&status, &n) != nil {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		counts[status] = n
	}
	if rows.Err() != nil {
		rawinput.Error(c, canonical.Unavailable())
		return
	}
	status := "pending"
	if counts["recovered"] == len(ids) {
		status = "completed"
	}
	c.JSON(200, gin.H{"job_id": c.Param("id"), "status": status, "counts": counts})
}
