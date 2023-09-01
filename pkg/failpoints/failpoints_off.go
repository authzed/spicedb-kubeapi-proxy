//go:build !failpoints
// +build !failpoints

package failpoints

// FailPoint does nothing in non-test builds
func FailPoint(_ string) {}
