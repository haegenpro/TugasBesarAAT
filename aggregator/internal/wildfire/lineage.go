package wildfire

import (
	"aggregator/internal/domain"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
)

func sortedIDs(fires map[string]Fire, activeOnly bool) []string {
	ids := []string{}
	for id, f := range fires {
		if !activeOnly || f.Active {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
func intersects(a, b []string, aliases map[string]string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y || aliases[x] == y {
				return true
			}
		}
	}
	return false
}
func sameMembers(a, b []string) bool { return reflect.DeepEqual(a, b) }
func unique(xs []string) []string {
	set := map[string]bool{}
	for _, x := range xs {
		set[x] = true
	}
	out := []string{}
	for x := range set {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}
func restorationBefore(a, b Fire) bool {
	if *a.DeactivatedAt != *b.DeactivatedAt {
		return *a.DeactivatedAt > *b.DeactivatedAt
	}
	if a.Derivation.BirthOffset != b.Derivation.BirthOffset {
		return a.Derivation.BirthOffset < b.Derivation.BirthOffset
	}
	if a.Derivation.Namespace != b.Derivation.Namespace {
		return a.Derivation.Namespace < b.Derivation.Namespace
	}
	return a.Derivation.EarliestKey < b.Derivation.EarliestKey
}

// Each transition region is the connected predecessor/successor bipartite
// graph. Mixed split+merge regions are reported rather than inventing precedence.
func resolve(namespace string, offset int64, components [][]string, obs map[string]Observation, old map[string]Fire, aliases map[string]string, provinces []Province) (map[string]Fire, []Intent, []Diagnostic) {
	next := map[string]Fire{}
	for id, f := range old {
		next[id] = f
	}
	parents := sortedIDs(old, true)
	children := make([][]int, len(parents))
	preds := make([][]int, len(components))
	for i, id := range parents {
		for j, c := range components {
			if intersects(old[id].Members, c, aliases) {
				children[i] = append(children[i], j)
				preds[j] = append(preds[j], i)
			}
		}
	}
	seenP, seenC := map[int]bool{}, map[int]bool{}
	diagnostics := []Diagnostic{}
	region := func(ps, cs []int) {
		sort.Ints(ps)
		sort.Ints(cs)
		keys := []string{}
		ids := []string{}
		split, merge := false, false
		for _, i := range ps {
			ids = append(ids, parents[i])
			split = split || len(children[i]) > 1
		}
		for _, j := range cs {
			keys = append(keys, components[j]...)
			merge = merge || len(preds[j]) > 1
		}
		keys = unique(keys)
		code := ""
		events := map[int]*Event{}
		if split && merge {
			code = "ambiguous_merge_split"
		} else {
			for _, j := range cs {
				event, err := mapEvent(components[j], obs, provinces)
				if err != nil {
					code = err.Error()
					break
				}
				events[j] = event
			}
		}
		if code != "" {
			diagnostics = append(diagnostics, Diagnostic{Code: code, Keys: keys, Predecessors: append([]string{}, ids...)})
			return
		}
		// Historical candidates belong to the region's retained identity graph, or
		// share retained observation membership after complete elimination.
		relevant := map[string]bool{}
		for _, id := range ids {
			relevant[id] = true
		}
		for id, f := range old {
			for _, j := range cs {
				if intersects(f.Members, components[j], aliases) {
					relevant[id] = true
				}
			}
		}
		for changed := true; changed; {
			changed = false
			for id, f := range old {
				if relevant[id] {
					for _, other := range f.Related {
						if !relevant[other] {
							relevant[other] = true
							changed = true
						}
					}
				}
			}
		}
		assigned := map[int]string{}
		used := map[string]bool{}
		for _, j := range cs {
			candidates := []Fire{}
			for _, id := range sortedIDs(old, false) {
				f := old[id]
				if !f.Active && relevant[id] && f.DeactivatedAt != nil && f.Derivation.Namespace == namespace && sameMembers(f.Members, components[j]) {
					candidates = append(candidates, f)
				}
			}
			sort.Slice(candidates, func(a, b int) bool { return restorationBefore(candidates[a], candidates[b]) })
			if len(candidates) > 0 {
				assigned[j] = candidates[0].ID
				used[candidates[0].ID] = true
			}
		}
		claims := map[int][]int{}
		for _, i := range ps {
			for _, j := range children[i] {
				if assigned[j] == "" {
					claims[j] = append(claims[j], i)
					break
				}
			}
		}
		for _, j := range cs {
			if assigned[j] != "" {
				continue
			}
			claimants := claims[j]
			if len(claimants) > 0 {
				anchor := func(i int) string {
					members := []string{}
					for _, k := range components[j] {
						for _, oldKey := range old[parents[i]].Members {
							if k == oldKey || aliases[oldKey] == k {
								members = append(members, k)
								break
							}
						}
					}
					return earliest(members, obs)
				}
				sort.Slice(claimants, func(a, b int) bool { return earlier(obs[anchor(claimants[a])], obs[anchor(claimants[b])]) })
				assigned[j] = parents[claimants[0]]
				used[assigned[j]] = true
			} else {
				derivation := Derivation{namespace, earliest(components[j], obs), offset}
				id, _ := Identity(derivation)
				if f, ok := next[id]; ok && f.Derivation != derivation {
					diagnostics = append(diagnostics, Diagnostic{Code: "identity_collision", Keys: keys, Predecessors: ids})
					return
				}
				assigned[j] = id
				used[id] = true
			}
		}
		related := append([]string{}, ids...)
		for _, id := range assigned {
			related = append(related, id)
		}
		related = unique(related)
		for _, i := range ps {
			id := parents[i]
			if !used[id] {
				f := next[id]
				f.Active = false
				f.LogicalRevision++
				at := offset
				f.DeactivatedAt = &at
				f.Related = unique(append(append([]string{}, f.Related...), related...))
				next[id] = f
			}
		}
		for _, j := range cs {
			id := assigned[j]
			f, exists := next[id]
			if !exists {
				f = Fire{Source: domain.SourceSatelit, ID: id, Derivation: Derivation{namespace, earliest(components[j], obs), offset}, LogicalRevision: 0, Related: []string{}}
			}
			if !f.Active || !reflect.DeepEqual(f.Event, events[j]) {
				f.LogicalRevision++
			}
			f.Active = true
			f.DeactivatedAt = nil
			f.Members = append([]string{}, components[j]...)
			f.Event = events[j]
			f.Related = unique(append(append([]string{}, f.Related...), related...))
			next[id] = f
		}
	}
	var visitP func(int, *[]int, *[]int)
	var visitC func(int, *[]int, *[]int)
	visitP = func(i int, ps, cs *[]int) {
		if seenP[i] {
			return
		}
		seenP[i] = true
		*ps = append(*ps, i)
		for _, j := range children[i] {
			visitC(j, ps, cs)
		}
	}
	visitC = func(j int, ps, cs *[]int) {
		if seenC[j] {
			return
		}
		seenC[j] = true
		*cs = append(*cs, j)
		for _, i := range preds[j] {
			visitP(i, ps, cs)
		}
	}
	for i := range parents {
		if !seenP[i] {
			ps, cs := []int{}, []int{}
			visitP(i, &ps, &cs)
			region(ps, cs)
		}
	}
	for j := range components {
		if !seenC[j] {
			ps, cs := []int{}, []int{}
			visitC(j, &ps, &cs)
			region(ps, cs)
		}
	}
	intents := []Intent{}
	batchID := digest("fire-batch-v1:", "["+quote(namespace)+","+strconv.FormatInt(offset, 10)+"]")
	for _, id := range sortedIDs(next, false) {
		f := next[id]
		previous, exists := old[id]
		if exists && previous.LogicalRevision == f.LogicalRevision {
			continue
		}
		kind := "upsert"
		var event *Event = f.Event
		if !f.Active {
			kind = "retract"
			event = nil
		}
		operationID := digest("fire-op-v1:", "["+quote(batchID)+","+quote(id)+","+quote(kind)+"]")
		intents = append(intents, Intent{OperationID: operationID, Source: domain.SourceSatelit, SourceRefID: id, Operation: kind, SourceOrder: SourceOrder{namespace + ":wildfire-output-v1", offset}, LogicalRevision: f.LogicalRevision, Event: event})
	}
	sort.Slice(diagnostics, func(i, j int) bool {
		a, _ := json.Marshal(diagnostics[i])
		b, _ := json.Marshal(diagnostics[j])
		return string(a) < string(b)
	})
	return next, intents, diagnostics
}
