//go:build !windows && !linux

package wildfireprocessor

import "fmt"

func peakResidentBytes() (uint64, error) {
	return 0, fmt.Errorf("peak RSS measurement is not implemented for this test host")
}
