package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

type legacyReplayRequest struct {
	Messages []string `json:"message_ids"`
	Adapter  string   `json:"adapter_version"`
}

func (s *Store) RegisterLegacyReplay(r gin.IRoutes, token string) {
	r.POST("/internal/ingestion/legacy-replays", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		var req legacyReplayRequest
		if err := canonical.Decode(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20), &req); err != nil || len(req.Messages) < 1 || len(req.Messages) > 1000 {
			rawinput.Error(c, canonical.Fail(400, "invalid_request", "replay requires 1..1000 message IDs"))
			return
		}
		if req.Adapter != clients.VolcanicAdapterName() {
			rawinput.Error(c, canonical.Fail(409, "adapter_mismatch", "the active PVMBG adapter is "+clients.VolcanicAdapterName()))
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
		defer cancel()
		selected, err := s.RequestLegacyReplay(ctx, req.Messages, req.Adapter)
		if err != nil {
			rawinput.Error(c, err)
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"selected": selected, "status": "pending"})
	})
	r.POST("/internal/ingestion/legacy-replays/status", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		var req legacyReplayRequest
		if err := canonical.Decode(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20), &req); err != nil || len(req.Messages) < 1 || len(req.Messages) > 1000 {
			rawinput.Error(c, canonical.Fail(400, "invalid_request", "status requires 1..1000 message IDs"))
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
		defer cancel()
		counts, err := s.LegacyReplayStatus(ctx, req.Messages)
		if err != nil {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		c.JSON(http.StatusOK, gin.H{"counts": counts})
	})
}

func (s *Store) RequestLegacyReplay(ctx context.Context, ids []string, adapter string) (int64, error) {
	var known int64
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM failed_ingestions WHERE source='PVMBG' AND message_id::text=ANY($1)`, ids).Scan(&known); err != nil {
		return 0, canonical.Unavailable()
	}
	if known != int64(len(ids)) {
		return 0, canonical.Fail(400, "invalid_request", "replay member unknown or not a PVMBG failure")
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE failed_ingestions SET status='replaying',replay_adapter=$2 WHERE source='PVMBG' AND message_id::text=ANY($1) AND status IN ('parked','retrying')`, ids, adapter)
	if err != nil {
		return 0, canonical.Unavailable()
	}
	return tag.RowsAffected(), nil
}

func (s *Store) NextLegacyReplay(ctx context.Context) (*clients.ParkedPayload, error) {
	var p clients.ParkedPayload
	err := s.Pool.QueryRow(ctx, `SELECT message_id::text,entity,raw_payload,truncated FROM failed_ingestions WHERE source='PVMBG' AND status='replaying' ORDER BY received_at,message_id LIMIT 1`).Scan(&p.ID, &p.Entity, &p.Payload, &p.Truncated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) FinishLegacyReplay(ctx context.Context, id string, failure error) error {
	if failure == nil {
		_, err := s.Pool.Exec(ctx, `UPDATE failed_ingestions SET status='recovered',recovered_at=now(),last_seen_at=now() WHERE message_id::text=$1 AND status='replaying'`, id)
		return err
	}
	_, err := s.Pool.Exec(ctx, `UPDATE failed_ingestions SET status='parked',reason=$2,attempts=attempts+1,last_seen_at=now() WHERE message_id::text=$1 AND status='replaying'`, id, failure.Error())
	return err
}

func (s *Store) LegacyReplayStatus(ctx context.Context, ids []string) (map[string]int, error) {
	counts := map[string]int{"selected": len(ids), "recovered": 0, "retrying": 0, "parked": 0}
	rows, err := s.Pool.Query(ctx, `SELECT CASE WHEN status='replaying' THEN 'retrying' ELSE status END,count(*) FROM failed_ingestions WHERE message_id::text=ANY($1) GROUP BY 1`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err = rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[status] += n
	}
	return counts, rows.Err()
}
