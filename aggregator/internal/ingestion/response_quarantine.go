package ingestion

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// QuarantineResponse records a rejected HTTP envelope without assigning any
// event position or moving a cursor/generation. Its nonce gives idempotent
// retention across lost acknowledgements and ownership turnover.
func (s *SourceStore) QuarantineResponse(ctx context.Context, id, media, reason string, body io.Reader) error {
	if id == "" || reason == "" || body == nil {
		return canonical.Fail(400, "invalid_request", "response identity, reason and complete body required")
	}
	owner, err := acquisition.Token(ctx, s.Source)
	if err != nil {
		return err
	}
	message := rawinput.Identity("http-response", s.Source, s.Namespace, id)
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	if err = s.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.Source(s.Source)}}); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(726118)`); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_failure_payloads WHERE namespace=$1 AND message_id=$2)`, s.Namespace, message).Scan(&exists); err != nil {
		return err
	}
	if exists {
		_ = tx.Rollback(ctx)
		return s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_failure_payloads WHERE namespace=$1 AND message_id=$2)`, s.Namespace, message)
	}
	var used int64
	if err = tx.QueryRow(ctx, `SELECT coalesce(sum(CASE WHEN chunked THEN size ELSE octet_length(payload) END),0) FROM ingestion_failure_payloads`).Scan(&used); err != nil {
		return err
	}
	quota := int64(platform.Int("FAILED_INPUT_QUOTA_BYTES", 256<<20))
	if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failures(namespace,message_id,source,position,received_at,reason,classification,status,attempts)VALUES($1,$2,$3,NULL,clock_timestamp(),$4,'incompatible','parked',1)`, s.Namespace, message, s.Source, reason); err != nil {
		return err
	}
	sum, size, ordinal := sha256.New(), int64(0), 0
	reader := &responseReader{Reader: body}
	buf := make([]byte, 64<<10)
	var inline []byte
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			size += int64(n)
			if size > quota-used {
				return canonical.Fail(503, "capacity_exhausted", "failed-input quota exhausted; source progress unchanged")
			}
			sum.Write(buf[:n])
			if ordinal == 0 && (readErr == io.EOF || readErr == io.ErrUnexpectedEOF && reader.last == io.EOF) {
				inline = append([]byte{}, buf[:n]...)
			} else {
				if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failure_payload_chunks(namespace,message_id,ordinal,data)VALUES($1,$2,$3,$4)`, s.Namespace, message, ordinal, buf[:n]); err != nil {
					return err
				}
			}
			ordinal++
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF && reader.last == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("incomplete response quarantine rolled back: %w", readErr)
		}
		if err = ctx.Err(); err != nil {
			return err
		}
	}
	chunked := inline == nil && size > 0
	if inline == nil {
		inline = []byte{}
	}
	digest := hex.EncodeToString(sum.Sum(nil))
	if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failure_payloads(namespace,message_id,content_type,payload,sha256,chunked,size)VALUES($1,$2,$3,$4,$5,$6,$7)`, s.Namespace, message, media, inline, digest, chunked, size); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failure_attempts(namespace,message_id,attempt,reason)VALUES($1,$2,1,$3)`, s.Namespace, message, reason); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_failure_payloads WHERE namespace=$1 AND message_id=$2 AND sha256=$3 AND size=$4)`, s.Namespace, message, digest, size)
}

type responseReader struct {
	io.Reader
	last error
}

func (r *responseReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.last = err
	return n, err
}

// WriteQuarantinedPayload serves both old inline failures and complete chunked
// HTTP bodies through the existing authenticated Ops inspection path.
func (s *Store) WriteQuarantinedPayload(ctx context.Context, namespace, id string, out io.Writer) (int64, error) {
	var inline []byte
	var chunked bool
	var size int64
	var digest string
	if err := s.DB.Pool.QueryRow(ctx, `SELECT payload,chunked,size,sha256 FROM ingestion_failure_payloads WHERE namespace=$1 AND message_id=$2`, namespace, id).Scan(&inline, &chunked, &size, &digest); err != nil {
		return 0, err
	}
	if !chunked {
		n, err := out.Write(inline)
		if err == nil && n != len(inline) {
			err = io.ErrShortWrite
		}
		return int64(n), err
	}
	rows, err := s.DB.Pool.Query(ctx, `SELECT ordinal,data FROM ingestion_failure_payload_chunks WHERE namespace=$1 AND message_id=$2 ORDER BY ordinal`, namespace, id)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	written := int64(0)
	ordinal := 0
	sum := sha256.New()
	for rows.Next() {
		var index int
		var data []byte
		if err = rows.Scan(&index, &data); err != nil {
			return written, err
		}
		if index != ordinal {
			return written, errors.New("failure payload chunk gap")
		}
		ordinal++
		n, err := out.Write(data)
		written += int64(n)
		sum.Write(data[:n])
		if err != nil {
			return written, err
		}
		if n != len(data) {
			return written, io.ErrShortWrite
		}
	}
	if err = rows.Err(); err != nil {
		return written, err
	}
	if written != size || hex.EncodeToString(sum.Sum(nil)) != digest {
		return written, fmt.Errorf("failure payload integrity mismatch")
	}
	return written, nil
}
