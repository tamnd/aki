//go:build !linux

package drivers

import "errors"

// The reactor is raw epoll and Linux only (doc 08 section 4.5's platform
// matrix): darwin and windows stay goroutine-driver territory, and there is
// deliberately no kqueue twin. Listen answers a NetReactor request here with
// an error, which its caller turns into the logged fallback notice; the
// goroutine driver then serves as if the knob had never been set, and
// NetStats reports the driver actually running.
func newReactorBackend(*Server, Options) (netBackend, error) {
	return nil, errors.New("no epoll on this platform")
}
