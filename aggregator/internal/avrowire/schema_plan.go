package avrowire

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Plans are immutable and shared with the cached writer schema. Avro performs
// structural typing; this layer applies explicitly declared application rules.
type schemaPlan struct {
	kind, name, logical string
	scale               int
	fields              []planField
	items               *schemaPlan
	base                *schemaPlan
	branches            []*schemaPlan
	rules               *validationRules
}
type planField struct {
	name  string
	node  *schemaPlan
	rules *validationRules
}
type validationRules struct {
	min, max *big.Rat
	allowed  map[string]map[any]bool
}
type planCompiler struct{ names map[string]*schemaPlan }

func compilePlan(text string) (*schemaPlan, error) {
	var raw any
	d := json.NewDecoder(strings.NewReader(text))
	d.UseNumber()
	if err := d.Decode(&raw); err != nil {
		return nil, err
	}
	p, err := (&planCompiler{names: map[string]*schemaPlan{}}).node(raw, "", 0)
	if err != nil {
		return nil, fmt.Errorf("Avro validation schema: %w", err)
	}
	return p, nil
}
func (c *planCompiler) node(raw any, ns string, depth int) (*schemaPlan, error) {
	if depth > 128 {
		return nil, fmt.Errorf("schema nesting exceeds 128")
	}
	switch x := raw.(type) {
	case string:
		switch x {
		case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
			return &schemaPlan{kind: x, name: x}, nil
		}
		name := x
		if !strings.Contains(name, ".") && ns != "" {
			name = ns + "." + name
		}
		if n := c.names[name]; n != nil {
			return n, nil
		}
		if n := c.names[x]; n != nil {
			return n, nil
		}
		return nil, fmt.Errorf("unknown named type %q", x)
	case []any:
		n := &schemaPlan{kind: "union"}
		for _, b := range x {
			p, e := c.node(b, ns, depth+1)
			if e != nil {
				return nil, e
			}
			n.branches = append(n.branches, p)
		}
		return n, nil
	case map[string]any:
		typ, ok := x["type"].(string)
		if !ok {
			p, e := c.node(x["type"], ns, depth+1)
			if e != nil {
				return nil, e
			}
			copy := *p
			copy.rules, e = declaredRules(x, p)
			return &copy, e
		}
		n := &schemaPlan{kind: typ, name: typ}
		n.logical, _ = x["logicalType"].(string)
		if s, ok := x["scale"].(json.Number); ok {
			v, _ := strconv.Atoi(string(s))
			n.scale = v
		}
		switch typ {
		case "record", "enum", "fixed":
			name, _ := x["name"].(string)
			if declared, ok := x["namespace"].(string); ok {
				ns = declared
			}
			if strings.Contains(name, ".") {
				ns = name[:strings.LastIndex(name, ".")]
			} else if ns != "" {
				name = ns + "." + name
			}
			n.name = name
			c.names[name] = n
			if typ == "record" {
				fields, ok := x["fields"].([]any)
				if !ok {
					return nil, fmt.Errorf("invalid record fields")
				}
				for _, f := range fields {
					field, ok := f.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("invalid field")
					}
					name, _ := field["name"].(string)
					p, e := c.node(field["type"], ns, depth+1)
					if e != nil {
						return nil, e
					}
					r, e := declaredRules(field, p)
					if e != nil {
						return nil, fmt.Errorf("field %s: %w", name, e)
					}
					n.fields = append(n.fields, planField{name, p, r})
				}
			}
		case "array", "map":
			key := "items"
			if typ == "map" {
				key = "values"
			}
			p, e := c.node(x[key], ns, depth+1)
			if e != nil {
				return nil, e
			}
			n.items = p
		default:
			p, e := c.node(typ, ns, depth+1)
			if e != nil {
				return nil, e
			}
			copy := *p
			n = &copy
			if p.kind != typ {
				n.base = p
			} else {
				n.logical, _ = x["logicalType"].(string)
			}
			if scale, ok := x["scale"].(json.Number); ok {
				n.scale, _ = strconv.Atoi(string(scale))
			}
		}
		var e error
		n.rules, e = declaredRules(x, n)
		return n, e
	}
	return nil, fmt.Errorf("invalid schema node")
}
func leaves(n *schemaPlan) []*schemaPlan {
	if n.kind == "union" {
		return n.branches
	}
	return []*schemaPlan{n}
}
func numeric(k string) bool { return k == "int" || k == "long" || k == "float" || k == "double" }
func declaredRules(object map[string]any, n *schemaPlan) (*validationRules, error) {
	raw, exists := object["x-validation"]
	if !exists {
		return nil, nil
	}
	if raw == nil {
		return nil, fmt.Errorf("x-validation must be an object")
	}
	return compileRules(raw, n)
}

func compileRules(raw any, n *schemaPlan) (*validationRules, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("x-validation must be an object")
	}
	r := &validationRules{}
	for key, v := range m {
		switch key {
		case "minimum", "maximum":
			num, ok := v.(json.Number)
			if !ok {
				return nil, fmt.Errorf("%s must be numeric", key)
			}
			rat, ok := new(big.Rat).SetString(string(num))
			if !ok {
				return nil, fmt.Errorf("invalid %s", key)
			}
			if key == "minimum" {
				r.min = rat
			} else {
				r.max = rat
			}
		case "allowedValues":
			values, ok := v.([]any)
			if !ok || len(values) == 0 {
				return nil, fmt.Errorf("allowedValues must be a nonempty scalar array")
			}
			r.allowed = map[string]map[any]bool{}
			for _, v := range values {
				matched := false
				for _, p := range leaves(n) {
					converted, e := ruleScalar(v, p.kind)
					if e == nil {
						if r.allowed[p.kind] == nil {
							r.allowed[p.kind] = map[any]bool{}
						}
						r.allowed[p.kind][converted] = true
						matched = true
					}
				}
				if !matched {
					return nil, fmt.Errorf("allowedValues member %v does not match the scalar schema", v)
				}
			}
		default:
			return nil, fmt.Errorf("unknown x-validation rule %q", key)
		}
	}
	if r.min != nil && r.max != nil && r.min.Cmp(r.max) > 0 {
		return nil, fmt.Errorf("minimum exceeds maximum")
	}
	if len(m) > 0 {
		nonNull := false
		for _, p := range leaves(n) {
			nonNull = nonNull || p.kind != "null"
		}
		if !nonNull {
			return nil, fmt.Errorf("constraints require a non-null scalar type")
		}
	}
	for _, p := range leaves(n) {
		if p.kind == "null" {
			continue
		}
		if p.logical != "" {
			return nil, fmt.Errorf("constraints on logical types are unsupported")
		}
		if p.kind == "float" || p.kind == "double" {
			for _, bound := range []*big.Rat{r.min, r.max} {
				if bound == nil {
					continue
				}
				v, _ := bound.Float64()
				if p.kind == "float" {
					v = float64(float32(v))
				}
				if math.IsInf(v, 0) {
					return nil, fmt.Errorf("numeric bound is not finite in %s", p.kind)
				}
			}
		}
		if (r.min != nil || r.max != nil) && !numeric(p.kind) {
			return nil, fmt.Errorf("numeric bounds incompatible with %s", p.kind)
		}
		if r.allowed != nil && !numeric(p.kind) && p.kind != "string" && p.kind != "boolean" && p.kind != "enum" {
			return nil, fmt.Errorf("allowedValues incompatible with %s", p.kind)
		}
	}
	return r, nil
}
func ruleScalar(v any, kind string) (any, error) {
	if s, ok := v.(string); ok && (kind == "string" || kind == "enum") {
		return s, nil
	}
	if b, ok := v.(bool); ok && kind == "boolean" {
		return b, nil
	}
	num, ok := v.(json.Number)
	if !ok || !numeric(kind) {
		return nil, fmt.Errorf("wrong scalar type")
	}
	if kind == "int" || kind == "long" {
		r, ok := new(big.Rat).SetString(string(num))
		if !ok || !r.IsInt() || !r.Num().IsInt64() {
			return nil, fmt.Errorf("invalid integer")
		}
		v := r.Num().Int64()
		if kind == "int" {
			if v < math.MinInt32 || v > math.MaxInt32 {
				return nil, fmt.Errorf("int overflow")
			}
			return int32(v), nil
		}
		return v, nil
	}
	bits := 64
	if kind == "float" {
		bits = 32
	}
	fv, e := strconv.ParseFloat(string(num), bits)
	if e != nil || math.IsNaN(fv) || math.IsInf(fv, 0) {
		return nil, fmt.Errorf("invalid float")
	}
	if bits == 32 {
		return float32(fv), nil
	}
	return fv, nil
}
func numberRat(v any) (*big.Rat, error) {
	switch x := v.(type) {
	case int32:
		return new(big.Rat).SetInt64(int64(x)), nil
	case int64:
		return new(big.Rat).SetInt64(x), nil
	case float32:
		return numberRat(float64(x))
	case float64:
		if !math.IsNaN(x) && !math.IsInf(x, 0) {
			return new(big.Rat).SetFloat64(x), nil
		}
	}
	return nil, fmt.Errorf("nonfinite or invalid numeric value")
}
func (r *validationRules) check(n *schemaPlan, v any, path string) error {
	if r == nil || v == nil {
		return nil
	}
	if r.allowed != nil && !r.allowed[n.kind][v] {
		return fmt.Errorf("%s: allowedValues rejects %v", path, v)
	}
	if r.min != nil || r.max != nil {
		value, e := numberRat(v)
		if e != nil {
			return fmt.Errorf("%s: %w", path, e)
		}
		for _, bound := range []struct {
			name    string
			v       *big.Rat
			minimum bool
		}{{"minimum", r.min, true}, {"maximum", r.max, false}} {
			if bound.v == nil {
				continue
			}
			b := bound.v
			// Bounds use the declared floating precision, avoiding decimal rounding artifacts.
			if n.kind == "float" || n.kind == "double" {
				f, _ := b.Float64()
				if n.kind == "float" {
					f = float64(float32(f))
				}
				if math.IsInf(f, 0) {
					return fmt.Errorf("%s: unrepresentable %s", path, bound.name)
				}
				b = new(big.Rat).SetFloat64(f)
			}
			cmp := value.Cmp(b)
			if (bound.minimum && cmp < 0) || (!bound.minimum && cmp > 0) {
				return fmt.Errorf("%s: %s rejects %v", path, bound.name, v)
			}
		}
	}
	return nil
}
func (n *schemaPlan) walk(v any, path string, depth int, fieldRules *validationRules) (any, error) {
	if depth > 256 {
		return nil, fmt.Errorf("%s: value nesting exceeds 256", path)
	}
	if n.base != nil {
		if e := n.rules.check(n, v, path); e != nil {
			return nil, e
		}
		return n.base.walk(v, path, depth+1, fieldRules)
	}
	if n.kind == "union" {
		if v == nil {
			for _, b := range n.branches {
				if b.kind == "null" {
					return nil, nil
				}
			}
			return nil, fmt.Errorf("%s: unexpected null", path)
		}
		wrapper, ok := v.(map[string]any)
		if !ok || len(wrapper) != 1 {
			return nil, fmt.Errorf("%s: invalid union", path)
		}
		for _, b := range n.branches {
			key := b.name
			if logicalBranch(b) {
				key = b.kind + "." + b.logical
			}
			if value, ok := wrapper[key]; ok {
				if e := n.rules.check(b, value, path); e != nil {
					return nil, e
				}
				return b.walk(value, path, depth+1, fieldRules)
			}
		}
		return nil, fmt.Errorf("%s: unknown union branch", path)
	}
	if e := n.rules.check(n, v, path); e != nil {
		return nil, e
	}
	if e := fieldRules.check(n, v, path); e != nil {
		return nil, e
	}
	if n.logical != "" {
		switch x := v.(type) {
		case time.Time:
			if n.logical == "date" {
				return x.UTC().Format("2006-01-02"), nil
			}
			return x.UTC().Format(time.RFC3339Nano), nil
		case time.Duration:
			if n.logical == "time-millis" {
				return int64(x / time.Millisecond), nil
			}
			return int64(x / time.Microsecond), nil
		case *big.Rat:
			return x.FloatString(n.scale), nil
		}
	}
	switch n.kind {
	case "record":
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: expected record", path)
		}
		out := map[string]any{}
		for _, f := range n.fields {
			x, e := f.node.walk(m[f.name], path+"."+f.name, depth+1, f.rules)
			if e != nil {
				return nil, e
			}
			if x != nil {
				out[f.name] = x
			}
		}
		return out, nil
	case "array":
		a, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("%s: expected array", path)
		}
		out := make([]any, len(a))
		for i, x := range a {
			y, e := n.items.walk(x, fmt.Sprintf("%s[%d]", path, i), depth+1, nil)
			if e != nil {
				return nil, e
			}
			out[i] = y
		}
		return out, nil
	case "map":
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: expected map", path)
		}
		out := map[string]any{}
		for k, x := range m {
			y, e := n.items.walk(x, path+"["+strconv.Quote(k)+"]", depth+1, nil)
			if e != nil {
				return nil, e
			}
			out[k] = y
		}
		return out, nil
	case "float", "double":
		if _, e := numberRat(v); e != nil {
			return nil, fmt.Errorf("%s: %w", path, e)
		}
	}
	return v, nil
}
func logicalBranch(n *schemaPlan) bool {
	switch n.logical {
	case "date":
		return n.kind == "int"
	case "time-millis":
		return n.kind == "int"
	case "time-micros", "timestamp-millis", "timestamp-micros":
		return n.kind == "long"
	case "decimal":
		return n.kind == "bytes"
	}
	return false
}
