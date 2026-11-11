package domain

import (
	"encoding/json"
	"fmt"
	"math"
)

type WildfireAttributes struct {
	DetectionCount int64    `json:"detection_count"`
	Satellites     []string `json:"satellites"`
	MaxConfidence  string   `json:"max_confidence"`
	TotalFRPMW     float64  `json:"total_frp_mw"`
	DetectionKeys  []string `json:"detection_keys"`
}

func ValidateWildfire(attrs map[string]any) error {
	b, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	var a WildfireAttributes
	if err = json.Unmarshal(b, &a); err != nil {
		return fmt.Errorf("invalid typed wildfire attributes: %w", err)
	}
	unique := func(xs []string) bool {
		seen := map[string]bool{}
		for _, s := range xs {
			if s == "" || seen[s] {
				return false
			}
			seen[s] = true
		}
		return true
	}
	if a.DetectionCount < 2 || int64(len(a.DetectionKeys)) != a.DetectionCount || len(a.Satellites) < 1 || !unique(a.Satellites) || !unique(a.DetectionKeys) || (a.MaxConfidence != "h" && a.MaxConfidence != "n") || a.TotalFRPMW < 0 || math.IsNaN(a.TotalFRPMW) || math.IsInf(a.TotalFRPMW, 0) {
		return fmt.Errorf("invalid wildfire count, membership, confidence or FRP")
	}
	return nil
}
