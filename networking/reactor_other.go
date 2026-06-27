//go:build !linux

package networking

// newReactor reports that the epoll event-loop reactor is unavailable on this
// platform. The reactor is built directly on Linux epoll (Spec/2064/reactor), so
// on every other operating system the server serves TCP connections on the
// goroutine-per-connection path regardless of the configured net mode.
func newReactor(s *Server) (netReactor, bool) { return nil, false }
