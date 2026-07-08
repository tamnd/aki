//go:build !linux

package f2srv

// serveWithReactor is a no-op off Linux: there is no epoll driver, so the caller
// serves every connection on its own goroutine. It reports handled false so
// ListenAndServe runs its goroutine accept loop on the already-bound listener.
func serveWithReactor(s *Server) (bool, error) {
	return false, nil
}
