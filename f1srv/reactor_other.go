//go:build !linux

package f1srv

// serveWithReactor is the non-Linux stub for the epoll event-loop driver. The reactor
// is Linux-and-TCP only (it owns raw fds and drives one epoll instance per loop), so on
// every other platform it reports "not handled" and ListenAndServe falls through to the
// portable goroutine-per-connection driver, even when NetMode is "reactor". That keeps
// the flag harmless off Linux rather than an error.
func serveWithReactor(s *Server) (handled bool, err error) {
	return false, nil
}
