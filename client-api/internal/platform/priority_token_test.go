package platform

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestPriorityHintCannotGrantFieldToTamperedExpiredOrRefreshTokens(t *testing.T) {
	key := strings.Repeat("k", 32)
	now := time.Unix(1700000000, 0)
	nonce := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	token := SignPriorityToken(nonce, "access", "field", key, now.Unix()+60)
	if !FieldPriorityToken(token, "access", key, now) {
		t.Fatal("valid scheduling hint rejected")
	}
	for _, candidate := range []string{token + "x", SignPriorityToken(nonce, "access", "field", key, now.Unix()), SignPriorityToken(nonce, "refresh", "field", key, now.Unix()+60), strings.Replace(SignPriorityToken(nonce, "access", "media", key, now.Unix()+60), ".media.", ".field.", 1), strings.Repeat("x", 300)} {
		if FieldPriorityToken(candidate, "access", key, now) {
			t.Fatal("invalid scheduling hint claimed field lane")
		}
	}
	if FieldPriorityToken(token, "access", strings.Repeat("z", 32), now) {
		t.Fatal("foreign signature accepted")
	}
}
