//go:build !unix

package store

// Off unix the arena backing stays a heap slice; see arena_map_unix.go for
// why the unix build maps it instead.

func arenaMap(n int) ([]byte, bool) {
	return make([]byte, n), false
}

func arenaUnmap([]byte) {}
