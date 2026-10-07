package domain

import "time"

type Notice struct {
	NoticeID          string         `json:"notice_id"`
	Source            Source         `json:"source"`
	SourceRefID       string         `json:"source_ref_id"`
	ObservationType   HazardType     `json:"observation_type"`
	AreaName          string         `json:"area_name"`
	OccurredAt        time.Time      `json:"occurred_at"`
	ReportedAt        time.Time      `json:"reported_at"`
	LastCheckedAt     *time.Time     `json:"last_checked_at,omitempty"`
	Revision          int64          `json:"revision"`
	State             string         `json:"state"`
	PendingDependency string         `json:"pending_dependency"`
	HazardID          string         `json:"hazard_id,omitempty"`
	Observation       map[string]any `json:"observation,omitempty"`
}

type LifecycleEvent struct {
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	RecordID       string          `json:"record_id"`
	Revision       int64           `json:"revision"`
	HazardRevision int64           `json:"hazard_revision,omitempty"`
	PublishedAt    time.Time       `json:"published_at"`
	CorrelationID  string          `json:"correlation_id"`
	Notice         *Notice         `json:"notice,omitempty"`
	Hazard         *HazardEvent    `json:"hazard,omitempty"`
	Status         *EndpointStatus `json:"source_status,omitempty"`
}

type EndpointStatus struct {
	Endpoint    string     `json:"endpoint"`
	State       string     `json:"state"`
	LastSuccess *time.Time `json:"last_success,omitempty"`
	LastAttempt *time.Time `json:"last_attempt,omitempty"`
	StaleSince  *time.Time `json:"stale_since,omitempty"`
	NextAttempt *time.Time `json:"next_attempt,omitempty"`
	Failures    int        `json:"failures"`
	Revision    int64      `json:"revision"`
}

type RecordMetadata struct {
	Revision        int64      `json:"revision"`
	LastSeenAt      *time.Time `json:"last_seen_at,omitempty"`
	ObservationTime time.Time  `json:"observation_time"`
	Historical      bool       `json:"historical"`
	State           string     `json:"state"`
	AssessmentState string     `json:"assessment_state"`
}
