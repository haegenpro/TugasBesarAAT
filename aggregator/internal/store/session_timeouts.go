package store

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultIdleTransactionTimeout = 15 * time.Second
const defaultAdmissionIdleTimeout = 30 * time.Second

type sessionTimeouts struct {
	transaction time.Duration
	admission   time.Duration
}

// A frozen client cannot run its own cleanup or heartbeat. PostgreSQL must
// expire both its idle data transaction and its separate admission session.
// These budgets do not resolve ambiguous commits or distributed decisions.
func loadSessionTimeouts() (sessionTimeouts, error) {
	result := sessionTimeouts{transaction: defaultIdleTransactionTimeout, admission: defaultAdmissionIdleTimeout}
	for _, setting := range []struct {
		name  string
		value *time.Duration
	}{
		{"DB_IDLE_TRANSACTION_TIMEOUT", &result.transaction},
		{"DB_ADMISSION_IDLE_TIMEOUT", &result.admission},
	} {
		if raw := os.Getenv(setting.name); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil {
				return result, fmt.Errorf("invalid %s", setting.name)
			}
			if value%time.Millisecond != 0 {
				return result, fmt.Errorf("%s must use whole milliseconds", setting.name)
			}
			*setting.value = value
		}
	}
	// The existing outbox holds a transaction while waiting on a bounded
	// ten-second publish attempt. Preserve that window and keep abandoned
	// sessions bounded below the source-takeover target's integration budget.
	if result.transaction <= 10*time.Second || result.transaction > 30*time.Second {
		return result, fmt.Errorf("DB_IDLE_TRANSACTION_TIMEOUT must be greater than 10s and at most 30s")
	}
	if result.admission < result.transaction+5*time.Second || result.admission > 45*time.Second {
		return result, fmt.Errorf("DB_ADMISSION_IDLE_TIMEOUT must exceed the transaction idle timeout by at least 5s and be at most 45s")
	}
	return result, nil
}

func timeoutMilliseconds(value time.Duration) string {
	return strconv.FormatInt(value.Milliseconds(), 10)
}

func (t sessionTimeouts) configureData(config *pgxpool.Config) {
	// Set this at connection startup, covering a freeze immediately after BEGIN
	// and before any transaction-local timeout could have been installed.
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = timeoutMilliseconds(t.transaction)
}

func (t sessionTimeouts) configureControl(config *pgxpool.Config) {
	t.configureData(config)
	// Outside a transaction, session advisory locks still remain held. The data
	// timeout above does not close these sessions, hence the separate setting.
	config.ConnConfig.RuntimeParams["idle_session_timeout"] = timeoutMilliseconds(t.admission)
	// Retire idle pooled connections before the server closes them. Connections
	// checked out by a frozen process deliberately remain subject to server expiry.
	config.MaxConnIdleTime = t.admission / 2
	config.HealthCheckPeriod = time.Second
}
