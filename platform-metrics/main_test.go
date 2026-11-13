package main

import (
	"math"
	"testing"
)

func TestResourceUnits(t *testing.T) {
	cases := []struct {
		raw  string
		cpu  bool
		want float64
	}{{"250m", true, .25}, {"100000000n", true, .1}, {"64Mi", false, 67108864}, {"1024Ki", false, 1048576}}
	for _, c := range cases {
		got, err := quantity(c.raw, c.cpu)
		if err != nil || math.Abs(got-c.want) > 1e-9 {
			t.Fatalf("unit %s: %g %v", c.raw, got, err)
		}
	}
	for _, raw := range []string{"NaN", "Inf", "-1", "bogus", "1Mi"} {
		if _, err := quantity(raw, true); err == nil {
			t.Fatalf("accepted invalid CPU %s", raw)
		}
	}
}
