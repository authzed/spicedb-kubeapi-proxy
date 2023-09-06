//go:build failpoints
// +build failpoints

package failpoints

import "sync"

type FailPoints struct {
	fps map[string]uint8
	sync.Mutex
}

var failpoints = FailPoints{
	fps: make(map[string]uint8),
}

// FailPoint will panic at its callsite if the name is in the list of enabled
// failpoints.
func FailPoint(failpoint string) {
	failpoints.Lock()
	defer failpoints.Unlock()

	remaining, ok := failpoints.fps[failpoint]
	if !ok {
		return
	}
	if remaining > 0 {
		failpoints.fps[failpoint] = remaining - 1
		panic(failpoint)
	} else {
		delete(failpoints.fps, failpoint)
	}
}

// EnableFailPoint enables a failpoint for n calls. After n calls, the failpoint
// is disabled.
func EnableFailPoint(failpoint string, n uint8) {
	failpoints.Lock()
	defer failpoints.Unlock()
	failpoints.fps[failpoint] = n
}

// DisableAll removes all failpoints.
func DisableAll() {
	failpoints.Lock()
	defer failpoints.Unlock()
	failpoints.fps = make(map[string]uint8, 0)
}
