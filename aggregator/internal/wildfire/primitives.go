package wildfire

import "strconv"

// ParseDetection and Connected share numerical/lexical primitives with the
// durable engine, without constructing the fixture-sized in-memory Model.
func ParseDetection(d Detection, offset int64, payload []byte) (Observation, error) {
	return parse(d, offset, payload)
}
func Connected(a, b Observation) bool {
	return a.Active && b.Active && a.Detection != nil && b.Detection != nil && a.Detection.Confidence != "l" && b.Detection.Confidence != "l" && edge(distance(a, b), a.EventTime.Sub(*b.EventTime))
}

// ResolveRegion requires components in earliest-event/key order and members in
// UTF-8 key order. The caller bounds this affected transition region; unrelated
// session history and the Model's historical result ledger are not loaded.
func ResolveRegion(namespace string, offset int64, components [][]string, observations map[string]Observation, predecessors map[string]Fire, aliases map[string]string, provinces []Province) (map[string]Fire, []Intent, []Diagnostic) {
	return resolveVersion(2, namespace, offset, components, observations, predecessors, aliases, provinces)
}

func BatchIdentity(namespace string, offset int64) string {
	return digest("fire-batch-v1:", "["+quote(namespace)+","+strconv.FormatInt(offset, 10)+"]")
}
