package domain

import "testing"

func TestWildfireAcceptsSameSatellitePair(t *testing.T) {
	attrs := map[string]any{"detection_count": 2, "satellites": []string{"N20"}, "max_confidence": "h", "total_frp_mw": 20, "detection_keys": []string{"a", "b"}}
	if err := ValidateWildfire(attrs); err != nil {
		t.Fatal(err)
	}
	attrs["satellites"] = []string{}
	if err := ValidateWildfire(attrs); err == nil {
		t.Fatal("accepted missing satellites")
	}
}
