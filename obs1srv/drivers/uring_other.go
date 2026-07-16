//go:build !linux

package drivers

import "errors"

// io_uring is Linux only (doc 08 section 4.5's platform matrix), same story
// as the reactor stub: Listen answers a NetURing request here with an error,
// its caller logs the fallback notice, the goroutine driver serves as if the
// knob had never been set, and NetStats reports the driver actually running.
func newURingBackend(*Server, Options) (netBackend, error) {
	return nil, errors.New("no io_uring on this platform")
}

// uringAvailable exists off Linux only so shared test helpers can ask; the
// answer is always no.
func uringAvailable() bool { return false }
