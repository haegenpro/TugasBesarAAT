package ingest

import (
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RetainResponse streams a rejected acquisition frame/page without admitting it
// to the source event sequence. Its source cursor remains unchanged for retry.
func (s *Store) RetainResponse(ctx context.Context, source, namespace, entity string, body io.Reader, reason string) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4031003)`); e != nil {
		return e
	}
	var used int64
	if e = tx.QueryRow(ctx, `SELECT (SELECT coalesce(sum(size),0) FROM response_failures)+(SELECT coalesce(sum(octet_length(payload)),0) FROM raw_inputs WHERE state='parked')`).Scan(&used); e != nil {
		return e
	}
	quota := int64(platform.Int("FAILED_INPUT_QUOTA_BYTES", 256<<20))
	id := uuid.NewString()
	if len(reason) > 1024 {
		reason = string([]rune(reason)[:min(len([]rune(reason)), 256)])
	}
	if _, e = tx.Exec(ctx, `INSERT INTO response_failures(message_id,source,namespace,entity,reason) VALUES($1,$2,$3,$4,$5)`, id, source, namespace, entity, reason); e != nil {
		return e
	}
	buffer := make([]byte, 64<<10)
	sum := sha256.New()
	size := int64(0)
	ordinal := 0
	reader := &evidenceReader{Reader: body}
	for {
		n, readErr := io.ReadFull(reader, buffer)
		if n > 0 {
			size += int64(n)
			if used+size > quota {
				return errors.New("response quarantine full; source cursor unchanged")
			}
			if _, e = tx.Exec(ctx, `INSERT INTO response_failure_chunks(message_id,ordinal,data) VALUES($1,$2,$3)`, id, ordinal, buffer[:n]); e != nil {
				return e
			}
			sum.Write(buffer[:n])
			ordinal++
		}
		if readErr == io.EOF || (readErr == io.ErrUnexpectedEOF && reader.last == io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("response evidence incomplete; capture rolled back: %w", readErr)
		}
		if e = ctx.Err(); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, `UPDATE response_failures SET size=$2,sha256=$3 WHERE message_id=$1`, id, size, hex.EncodeToString(sum.Sum(nil))); e != nil {
		return e
	}
	return commit(ctx, tx, "response_failure")
}

type evidenceReader struct {
	io.Reader
	last error
}

func (r *evidenceReader) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	r.last = e
	return n, e
}

func (s *Store) WriteResponsePayload(ctx context.Context, id string, out io.Writer) (int64, string, error) {
	var size int64
	var digest string
	if e := s.Pool.QueryRow(ctx, `SELECT size,sha256 FROM response_failures WHERE message_id=$1`, id).Scan(&size, &digest); e != nil {
		return 0, "", e
	}
	rows, e := s.Pool.Query(ctx, `SELECT data FROM response_failure_chunks WHERE message_id=$1 ORDER BY ordinal`, id)
	if e != nil {
		return 0, "", e
	}
	defer rows.Close()
	var written int64
	for rows.Next() {
		var data []byte
		if e = rows.Scan(&data); e != nil {
			return written, digest, e
		}
		n, err := out.Write(data)
		written += int64(n)
		if err != nil {
			return written, digest, err
		}
		if n != len(data) {
			return written, digest, io.ErrShortWrite
		}
	}
	if rows.Err() != nil {
		return written, digest, rows.Err()
	}
	if written != size {
		return written, digest, errors.New("response evidence size mismatch")
	}
	return written, digest, nil
}

func resolveResponse(ctx context.Context, tx pgx.Tx, source, namespace, entity string) error {
	_, e := tx.Exec(ctx, `UPDATE response_failures SET resolved=true WHERE source=$1 AND namespace=$2 AND entity=$3 AND NOT resolved`, source, namespace, entity)
	return e
}
