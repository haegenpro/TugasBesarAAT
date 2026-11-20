package avrowire

import (
	"context"
	"fmt"
	"slices"

	"github.com/linkedin/goavro/v2"
)

// Reader is local policy, never inferred from a message's writer schema ID.
// Its codec supplies defaults; its plan validates and normalizes resolved data.
type Reader struct {
	codec *goavro.Codec
	plan  *schemaPlan
}

func NewReader(text string) (*Reader, error) {
	codec, err := goavro.NewCodec(text)
	if err != nil {
		return nil, err
	}
	plan, err := compilePlan(text)
	if err != nil {
		return nil, err
	}
	return &Reader{codec: codec, plan: plan}, nil
}

// DecodeWithReader validates the writer before discarding writer-only fields.
// Defaults are applied and validated by the reader codec, then removed from
// the returned source projection if the writer did not send those fields.
func (c *Client) DecodeWithReader(ctx context.Context, framed []byte, reader *Reader) (any, error) {
	if reader == nil {
		return nil, fmt.Errorf("explicit reader schema required")
	}
	writer, native, err := c.decodeWriter(ctx, framed)
	if err != nil {
		return nil, err
	}
	if _, err = writer.plan.walk(native, "$", 0, nil); err != nil {
		return nil, err
	}
	if err := checkReadable(writer.plan, reader.plan, "$", map[schemaPair]bool{}); err != nil {
		return nil, err
	}
	resolved, present, err := resolve(writer.plan, reader.plan, native, "$", 0)
	if err != nil {
		return nil, err
	}
	// goavro supplies reader defaults and checks enum symbols/fixed sizes/types.
	b, err := reader.codec.BinaryFromNative(nil, resolved)
	if err != nil {
		return nil, fmt.Errorf("reader resolution: %w", err)
	}
	resolved, _, err = reader.codec.NativeFromBinary(b)
	if err != nil {
		return nil, err
	}
	value, err := reader.plan.walk(resolved, "$", 0, nil)
	if err != nil {
		return nil, err
	}
	return present.project(value), nil
}

// Presence follows records, arrays and maps recursively. A nil mask is a scalar
// (including explicit null); missing record keys are synthetic reader defaults.
type presence struct {
	fields map[string]*presence
	items  []*presence
}

func (p *presence) project(v any) any {
	if p == nil {
		return v
	}
	if p.fields != nil {
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		out := map[string]any{}
		for k, child := range p.fields {
			if x, ok := m[k]; ok {
				out[k] = child.project(x)
			}
		}
		return out
	}
	if p.items != nil {
		a, ok := v.([]any)
		if !ok {
			return v
		}
		out := make([]any, len(a))
		for i, x := range a {
			out[i] = p.items[i].project(x)
		}
		return out
	}
	return v
}

func branchKey(n *schemaPlan) string {
	if logicalBranch(n) {
		return n.kind + "." + n.logical
	}
	return n.name
}
func typeMatches(w, r *schemaPlan) bool {
	if w.kind == r.kind {
		switch w.kind {
		case "record", "enum", "fixed":
			return w.name == r.name || slices.Contains(r.aliases, w.name)
		}
		return true
	}
	switch w.kind {
	case "int":
		return r.kind == "long" || r.kind == "float" || r.kind == "double"
	case "long":
		return r.kind == "float" || r.kind == "double"
	case "float":
		return r.kind == "double"
	case "string":
		return r.kind == "bytes"
	case "bytes":
		return r.kind == "string"
	}
	return false
}

// resolve translates native union wrappers and projects records by reader
// field names/aliases. It has no source-specific field names or version logic.
func resolve(w, r *schemaPlan, v any, path string, depth int) (any, *presence, error) {
	fail := func(message string) (any, *presence, error) {
		return nil, nil, fmt.Errorf("%s: reader resolution: %s", path, message)
	}
	if depth > 256 {
		return fail("nesting exceeds 256")
	}
	if w.kind == "union" {
		for _, b := range w.branches {
			if v == nil && b.kind == "null" {
				return resolve(b, r, nil, path, depth+1)
			}
			if wrapper, ok := v.(map[string]any); ok {
				if x, ok := wrapper[branchKey(b)]; ok {
					return resolve(b, r, x, path, depth+1)
				}
			}
		}
		return fail("unknown writer union branch")
	}
	if r.kind == "union" {
		for _, b := range r.branches {
			if typeMatches(w, b) {
				x, p, err := resolve(w, b, v, path, depth+1)
				if err != nil {
					return nil, nil, err
				}
				if b.kind == "null" {
					return nil, p, nil
				}
				return goavro.Union(branchKey(b), x), p, nil
			}
		}
		return fail("no matching reader union branch")
	}
	if !typeMatches(w, r) {
		return fail(fmt.Sprintf("writer %s (%s) incompatible with reader %s (%s)", w.kind, w.name, r.kind, r.name))
	}
	switch r.kind {
	case "record":
		values, ok := v.(map[string]any)
		if !ok {
			return fail("expected record")
		}
		out := map[string]any{}
		p := &presence{fields: map[string]*presence{}}
		for _, rf := range r.fields {
			var wf *planField
			for i := range w.fields {
				f := &w.fields[i]
				if f.name == rf.name || slices.Contains(rf.aliases, f.name) {
					wf = f
					break
				}
			}
			if wf == nil {
				if !rf.hasDefault {
					return fail("missing reader field without default: " + rf.name)
				}
				continue
			}
			x, child, err := resolve(wf.node, rf.node, values[wf.name], path+"."+rf.name, depth+1)
			if err != nil {
				return nil, nil, err
			}
			out[rf.name] = x
			p.fields[rf.name] = child
		}
		return out, p, nil
	case "array":
		a, ok := v.([]any)
		if !ok {
			return fail("expected array")
		}
		out := make([]any, len(a))
		p := &presence{items: make([]*presence, len(a))}
		for i, x := range a {
			value, child, err := resolve(w.items, r.items, x, fmt.Sprintf("%s[%d]", path, i), depth+1)
			if err != nil {
				return nil, nil, err
			}
			out[i] = value
			p.items[i] = child
		}
		return out, p, nil
	case "map":
		m, ok := v.(map[string]any)
		if !ok {
			return fail("expected map")
		}
		out := map[string]any{}
		p := &presence{fields: map[string]*presence{}}
		for k, x := range m {
			value, child, err := resolve(w.items, r.items, x, path+"["+k+"]", depth+1)
			if err != nil {
				return nil, nil, err
			}
			out[k] = value
			p.fields[k] = child
		}
		return out, p, nil
	}
	if w.kind != r.kind {
		if logicalBranch(w) || logicalBranch(r) {
			return fail("logical type promotion requires compatible physical types")
		}
		switch r.kind {
		case "long":
			v = int64(v.(int32))
		case "float":
			switch x := v.(type) {
			case int32:
				v = float32(x)
			case int64:
				v = float32(x)
			}
		case "double":
			switch x := v.(type) {
			case int32:
				v = float64(x)
			case int64:
				v = float64(x)
			case float32:
				v = float64(x)
			}
		case "bytes":
			v = []byte(v.(string))
		case "string":
			v = string(v.([]byte))
		}
	}
	return v, nil, nil
}

// Check schema shape even when an array/map is empty. Union readability is
// branch-dependent; resolve checks the branch actually carried by each datum.
type schemaPair struct{ writer, reader *schemaPlan }

func checkReadable(w, r *schemaPlan, path string, visiting map[schemaPair]bool) error {
	pair := schemaPair{w, r}
	if visiting[pair] {
		return nil
	}
	visiting[pair] = true
	defer delete(visiting, pair)
	if w.kind == "union" {
		for _, b := range w.branches {
			if checkReadable(b, r, path, visiting) == nil {
				return nil
			}
		}
		return fmt.Errorf("%s: reader resolution: no readable writer union branch", path)
	}
	if r.kind == "union" {
		for _, b := range r.branches {
			if typeMatches(w, b) {
				return checkReadable(w, b, path, visiting)
			}
		}
		return fmt.Errorf("%s: reader resolution: no matching reader union branch", path)
	}
	if !typeMatches(w, r) {
		return fmt.Errorf("%s: reader resolution: incompatible writer %s and reader %s", path, w.name, r.name)
	}
	switch r.kind {
	case "array", "map":
		return checkReadable(w.items, r.items, path+"[]", visiting)
	case "record":
		for _, rf := range r.fields {
			var wf *planField
			for i := range w.fields {
				f := &w.fields[i]
				if f.name == rf.name || slices.Contains(rf.aliases, f.name) {
					wf = f
					break
				}
			}
			if wf == nil {
				if !rf.hasDefault {
					return fmt.Errorf("%s: reader resolution: missing reader field without default: %s", path, rf.name)
				}
				continue
			}
			if err := checkReadable(wf.node, rf.node, path+"."+rf.name, visiting); err != nil {
				return err
			}
		}
	}
	return nil
}
