package pipeline

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Session struct {
	Source    string `json:"source"`
	Namespace string `json:"namespace"`
	Initial   int64  `json:"initial_offset"`
}

func Start(parent context.Context, st *store.Store, registry *avrowire.Client, r *gin.Engine, readers map[string]*avrowire.SourceReader, logger *slog.Logger) (func(), error) {
	enabled := platform.Bool("RAW_PIPELINE_ENABLED", false)
	history := platform.Bool("SATELLITE_HISTORY_ENABLED", false)
	noop := func() {}
	if !enabled && !history {
		return noop, nil
	}
	if st.Processor == nil || st.Canonical.Durability == nil {
		return nil, fmt.Errorf("raw path requires platform processor authority and replicated durability")
	}
	token := os.Getenv("HISTORY_SERVICE_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("dedicated HISTORY_SERVICE_TOKEN required")
	}
	for _, key := range []string{"AUDIT_TOKEN", "PROCESSOR_SERVICE_TOKEN", "CANONICAL_SERVICE_TOKEN", "AGGREGATOR_ADMIN_TOKEN", "INGESTION_SERVICE_TOKEN", "BMKG_API_KEY", "PVMBG_TOKEN"} {
		if token == os.Getenv(key) {
			return nil, fmt.Errorf("dedicated history token required")
		}
	}
	var sessions []Session
	if err := canonical.Decode(strings.NewReader(os.Getenv("RAW_INPUT_SESSIONS")), &sessions); err != nil || len(sessions) == 0 {
		return nil, fmt.Errorf("RAW_INPUT_SESSIONS requires configured starts")
	}
	st.Inputs.Starts = map[string]int64{}
	sources := map[string]bool{}
	for _, session := range sessions {
		key := store.InputSession(session.Source, session.Namespace)
		if session.Namespace == "" || session.Initial < 0 || (session.Source != "BMKG" && session.Source != "SATELIT") || sources[session.Source] {
			return nil, fmt.Errorf("one explicit active session per source required")
		}
		sources[session.Source] = true
		st.Inputs.Starts[key] = session.Initial
	}
	if os.Getenv("INGESTION_BASE_URL") != "" {
		quota, err := strconv.ParseInt(os.Getenv("INPUT_PAYLOAD_QUOTA_BYTES"), 10, 64)
		if err != nil || quota < 1 || os.Getenv("INGESTION_SERVICE_TOKEN") == "" {
			return nil, fmt.Errorf("owner-store URL requires explicit quota and credential")
		}
		st.Inputs.Resolver = ingestion.HTTPResolver{Client: platform.Client(2 * time.Second), BaseURL: os.Getenv("INGESTION_BASE_URL"), Token: os.Getenv("INGESTION_SERVICE_TOKEN")}
		st.Inputs.PayloadQuota = quota
	}
	registry.SetCache(store.SchemaCache{Pool: st.Pool})
	if history {
		rawinput.HTTP{Repository: st, Token: token}.Register(r)
	}
	if !enabled {
		return noop, nil
	}
	if sources["BMKG"] && !platform.Bool("BMKG_RAW_ENABLED", false) {
		return nil, fmt.Errorf("raw BMKG requires disabling legacy BMKG through BMKG_RAW_ENABLED")
	}
	processor, ok := st.Processor.(canonical.OwnershipRepository)
	if !ok {
		return nil, fmt.Errorf("processor lease repository unavailable")
	}
	delivery, ok := st.Canonical.Fence.(canonical.OwnershipRepository)
	if !ok {
		return nil, fmt.Errorf("canonical delivery authority unavailable")
	}
	schema, err := os.ReadFile(os.Getenv("RAW_INPUT_SCHEMA"))
	if err != nil {
		return nil, err
	}
	reader, err := avrowire.NewReader(string(schema))
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(os.Getenv("NATS_URL"), nats.Timeout(2*time.Second), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	for _, session := range sessions {
		wg.Add(1)
		go func(session Session) {
			defer wg.Done()
			w := &Worker{Store: st, Registry: registry, Reader: reader, Readers: readers, Processor: processor, Delivery: delivery, JS: js, Logger: logger}
			w.Run(ctx, session.Source, session.Namespace)
		}(session)
	}
	return func() { cancel(); wg.Wait(); nc.Close() }, nil
}

// Keep the deployment sample a JSON value, never a command interpreted by a
// shell. This helper is also useful to validate private prepared configuration.
func SessionsJSON(sessions []Session) string { b, _ := json.Marshal(sessions); return string(b) }
