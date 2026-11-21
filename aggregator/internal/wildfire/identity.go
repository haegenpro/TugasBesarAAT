package wildfire

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

// Ordinary compact UTF-8 JSON, without Go's optional HTML/U+2028 escaping.
func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 32 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
func tupleJSON(d Derivation) string {
	return "[" + quote(d.Namespace) + "," + quote(d.EarliestKey) + "," + strconv.FormatInt(d.BirthOffset, 10) + "]"
}
func digest(prefix, raw string) string {
	return fmt.Sprintf("%s%x", prefix, sha256.Sum256([]byte(raw)))
}
func Identity(d Derivation) (string, error) {
	if !text(d.Namespace) || !text(d.EarliestKey) || d.BirthOffset < 0 {
		return "", fmt.Errorf("invalid derivation")
	}
	return digest("fire-v1:", tupleJSON(d)), nil
}
