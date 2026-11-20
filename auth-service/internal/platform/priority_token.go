package platform

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// Priority tokens carry a scheduling hint, never an authorization decision.
// Every request must still be introspected against the live sessions database.
func SignPriorityToken(nonce, kind, role, key string, expiry int64) string {
	body := strings.Join([]string{"p1", kind, role, strconv.FormatInt(expiry, 10), nonce}, ".")
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte("aat-auth-scheduling-v1\x00" + body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func FieldPriorityToken(token, kind, key string, now time.Time) bool {
	if len(key) < 32 || len(token) > 256 {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 6 || parts[0] != "p1" || parts[1] != kind || parts[2] != "field" {
		return false
	}
	expires, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || expires <= now.Unix() {
		return false
	}
	nonce, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil || len(nonce) != 32 {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[5])
	if err != nil || len(signature) != sha256.Size {
		return false
	}
	expected := SignPriorityToken(parts[4], kind, "field", key, expires)
	decoded, _ := base64.RawURLEncoding.DecodeString(strings.Split(expected, ".")[5])
	return hmac.Equal(signature, decoded)
}
