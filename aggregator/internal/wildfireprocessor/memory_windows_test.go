package wildfireprocessor

import (
	"fmt"
	"syscall"
	"unsafe"
)

// Windows reports the process high-water resident working set, not merely a
// Go heap sample. The scope is the current component-test process lifetime.
func peakResidentBytes() (uint64, error) {
	type counters struct {
		Size, Faults                                                                    uint32
		Peak, Working, PeakPaged, Paged, PeakNonpaged, Nonpaged, Pagefile, PeakPagefile uintptr
	}
	c := counters{}
	c.Size = uint32(unsafe.Sizeof(c))
	proc := syscall.NewLazyDLL("psapi.dll").NewProc("GetProcessMemoryInfo")
	ok, _, err := proc.Call(^uintptr(0), uintptr(unsafe.Pointer(&c)), uintptr(c.Size))
	if ok == 0 {
		return 0, fmt.Errorf("process memory counters: %v", err)
	}
	return uint64(c.Peak), nil
}
