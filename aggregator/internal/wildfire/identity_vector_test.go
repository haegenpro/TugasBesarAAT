package wildfire

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// scripts/m2/p4_burst_cluster.py recomputes BatchIdentity to join input positions with canonical
// operations; this pins the formula that both sides use.
func TestBatchIdentityMatchesTheDocumentedFormula(t *testing.T) {
	sum := sha256.Sum256([]byte(`["satellite-p4-x",5]`))
	if got, want := BatchIdentity("satellite-p4-x", 5), "fire-batch-v1:"+hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("BatchIdentity = %s, want %s", got, want)
	}
}
