package wildfire

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

type record struct {
	fingerprint string
	result      Result
}
type Model struct {
	config                Config
	provinces             []Province
	next                  int64
	frontier              *int64
	watermark, virtualNow *time.Time
	late                  uint64
	observations          map[string]Observation
	fires                 map[string]Fire
	aliases               map[string]string
	ledger                map[int64]record
	history               []Operation
	singletons, excluded  int
	diagnostics           []Diagnostic
}

func New(config Config) (*Model, error) {
	if config.Version != Version || !text(config.Namespace) || config.InitialOffset == nil || *config.InitialOffset < 0 || *config.InitialOffset >= math.MaxInt64 || config.AllowedLatenessSeconds == nil || *config.AllowedLatenessSeconds < 0 || *config.AllowedLatenessSeconds > math.MaxInt64/int64(time.Second) {
		return nil, fmt.Errorf("version, namespace, initial_offset and nonnegative allowed_lateness_seconds required")
	}
	if fmt.Sprintf("%x", sha256.Sum256(config.ProvinceJSON)) != config.ProvinceSHA256 {
		return nil, fmt.Errorf("province SHA-256 mismatch")
	}
	var provinces []Province
	if err := Decode(config.ProvinceJSON, &provinces); err != nil {
		return nil, err
	}
	if len(provinces) == 0 {
		return nil, fmt.Errorf("province table required")
	}
	ids := map[string]bool{}
	for _, p := range provinces {
		if !text(p.ID) || !text(p.Name) || ids[p.ID] || !validCoordinates(p.Latitude, p.Longitude) {
			return nil, fmt.Errorf("invalid province row or duplicate ID")
		}
		ids[p.ID] = true
	}
	// Copy config pointers/bytes so callers cannot change an established run.
	offset, lateness := *config.InitialOffset, *config.AllowedLatenessSeconds
	config.InitialOffset = &offset
	config.AllowedLatenessSeconds = &lateness
	config.ProvinceJSON = append([]byte(nil), config.ProvinceJSON...)
	return &Model{config: config, provinces: provinces, next: offset, observations: map[string]Observation{}, fires: map[string]Fire{}, aliases: map[string]string{}, ledger: map[int64]record{}, history: []Operation{}, diagnostics: []Diagnostic{}}, nil
}
func (m *Model) Apply(input Operation) (Result, error) {
	fail := func(code, message string) (Result, error) {
		return Result{}, &Error{Code: code, Message: message, ExpectedOffset: m.next}
	}
	if input.Version != Version || input.Namespace != m.config.Namespace || input.Offset < 0 || input.Offset >= math.MaxInt64 {
		return fail("invalid_input", "invalid operation version, namespace or offset")
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return fail("invalid_input", "operation must contain finite JSON values")
	}
	fp := fmt.Sprintf("%x", sha256.Sum256(raw))
	if old, ok := m.ledger[input.Offset]; ok {
		if old.fingerprint != fp {
			return fail("identity_conflict", "offset already contains different logical content or payload bytes")
		}
		return clone(old.result), nil
	}
	if input.Offset < m.next {
		return fail("stale_redelivery", "offset predates the retained session start")
	}
	if input.Offset != m.next {
		return fail("source_gap", "expected contiguous input")
	}
	if input.VirtualNow != nil && (input.VirtualNow.IsZero() || input.VirtualNow.Year() < 1 || input.VirtualNow.Year() > 9999) {
		return fail("invalid_input", "invalid virtual_now")
	}
	if input.TargetKey != "" && !text(input.TargetKey) {
		return fail("invalid_input", "invalid target key")
	}
	if input.Kind != "detection" && input.Kind != "update" && input.Kind != "retract" && input.Kind != "heartbeat" {
		return fail("invalid_input", "unknown operation kind")
	}
	if (input.Kind == "detection" || input.Kind == "update") != (input.Detection != nil) {
		return fail("invalid_input", "detection/update requires a complete detection; other kinds omit it")
	}
	if (input.Kind == "update" || input.Kind == "retract") != text(input.TargetKey) {
		return fail("invalid_input", "update/retract requires target_key; other kinds omit it")
	}
	input = clone(input)
	obs := map[string]Observation{}
	for k, o := range m.observations {
		obs[k] = o
	}
	aliases := map[string]string{}
	for k, v := range m.aliases {
		aliases[k] = v
	}
	var affected *Observation
	if input.Detection != nil {
		o, e := parse(*input.Detection, input.Offset, input.Payload)
		if e != nil {
			return fail("invalid_input", e.Error())
		}
		if old, ok := obs[o.Key]; ok && old.Detection != nil && old.Tuple != o.Tuple {
			return fail("identity_conflict", "same key has a different lexical derivation tuple")
		}
		if input.Kind == "update" {
			old, ok := obs[input.TargetKey]
			if !ok || !old.Active || old.Detection == nil {
				return fail("unknown_target", "update target is not an active observation")
			}
			if old.Key != o.Key {
				if existing, ok := obs[o.Key]; ok && existing.Active {
					return fail("identity_conflict", "replacement key is already active")
				}
				old.Active = false
				old.LastOffset = input.Offset
				old.Payload = append([]byte(nil), input.Payload...)
				obs[old.Key] = old
				for k, v := range aliases {
					if v == old.Key {
						aliases[k] = o.Key
					}
				}
				aliases[old.Key] = o.Key
				delete(aliases, o.Key)
			}
		}
		obs[o.Key] = o
		affected = &o
	} else if input.Kind == "retract" {
		old := obs[input.TargetKey]
		old.Key = input.TargetKey
		old.Active = false
		old.LastOffset = input.Offset
		old.Payload = append([]byte(nil), input.Payload...)
		obs[old.Key] = old
		affected = &old
	}
	late := affected != nil && affected.EventTime != nil && m.watermark != nil && affected.EventTime.Before(*m.watermark)
	virtualNow, watermark := m.virtualNow, m.watermark
	if input.VirtualNow != nil {
		sample := input.VirtualNow.UTC()
		input.VirtualNow = &sample
		if virtualNow == nil || sample.After(*virtualNow) {
			virtualNow = &sample
			w := sample.Add(-time.Duration(*m.config.AllowedLatenessSeconds) * time.Second)
			if w.Year() < 1 {
				return fail("invalid_input", "watermark outside supported UTC date range")
			}
			if watermark == nil || w.After(*watermark) {
				watermark = &w
			}
		}
	}
	components, singletons, excluded := graph(obs)
	fires, intents, diagnostics := resolve(m.config.Namespace, input.Offset, components, obs, m.fires, aliases, m.provinces)
	previous := m.watermark
	m.observations = obs
	m.aliases = aliases
	m.fires = fires
	m.singletons = singletons
	m.excluded = excluded
	m.diagnostics = diagnostics
	m.virtualNow = virtualNow
	m.watermark = watermark
	offset := input.Offset
	m.frontier = &offset
	m.next = offset + 1
	if late {
		m.late++
	}
	m.history = append(m.history, input)
	outcome := "applied"
	if len(diagnostics) > 0 {
		outcome = "blocked_output"
	} else if len(intents) == 0 {
		outcome = "no_change"
	}
	var batch *Batch
	if len(intents) > 0 {
		batch = &Batch{ID: digest("fire-batch-v1:", "["+quote(m.config.Namespace)+","+strconv.FormatInt(offset, 10)+"]"), Intents: intents}
	}
	result := Result{Version: Version, Offset: offset, Outcome: outcome, Late: late, PreviousWatermark: previous, Batch: batch, State: m.Snapshot()}
	m.ledger[offset] = record{fp, clone(result)}
	return clone(result), nil
}
func (m *Model) Snapshot() Snapshot {
	observations := []Observation{}
	keys := []string{}
	for key := range m.observations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		observations = append(observations, m.observations[key])
	}
	fires := []Fire{}
	for _, id := range sortedIDs(m.fires, false) {
		fires = append(fires, m.fires[id])
	}
	return clone(Snapshot{Version: Version, Algorithm: Algorithm, Namespace: m.config.Namespace, ProvinceSHA256: m.config.ProvinceSHA256, Frontier: m.frontier, Watermark: m.watermark, VirtualNow: m.virtualNow, LateMutations: m.late, Singletons: m.singletons, Excluded: m.excluded, Observations: observations, Fires: fires, Diagnostics: m.diagnostics, History: m.history})
}
