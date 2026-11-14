package wildfireprocessor

import "syscall"

func peakResidentBytes() (uint64, error) {
	var r syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return uint64(r.Maxrss) * 1024, err
}
