package command

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// This file implements the SLOWLOG log from doc 20 section 3: a ring of the most
// recent commands whose execution time crossed slowlog-log-slower-than. The
// recording hook runs from runCommand once a command has finished and its
// microsecond cost is known.

// slowlogArgMax caps how long a single logged argument may be. Longer arguments
// are truncated with a "..." suffix, the same as Redis.
const slowlogArgMax = 128

// slowlogArgCount caps how many arguments are kept. Past the cap the remaining
// arguments are folded into one "... (N more arguments)" marker.
const slowlogArgCount = 32

// slowlogEntry is one recorded slow command, the data behind a SLOWLOG GET row.
type slowlogEntry struct {
	id    int64
	ts    int64
	durUs int64
	args  []string
	addr  string
	name  string
}

// slowlogState holds the slow command ring and the global id counter. The ring is
// guarded by its own mutex; the id counter is atomic so the next id can be taken
// without holding the ring lock.
type slowlogState struct {
	mu      sync.Mutex
	entries []slowlogEntry
	nextID  atomic.Int64
}

// slowlogMaybeAdd records a command in the slow log when its execution time is at
// or above slowlog-log-slower-than. A threshold of -1 disables the log and 0
// records every command. argv is the command as the client sent it.
func (d *Dispatcher) slowlogMaybeAdd(c connInfo, argv [][]byte, durUs int64) {
	threshold := int64(10000)
	if d.conf != nil {
		threshold = d.conf.slowlogThreshold()
	}
	if threshold < 0 || durUs < threshold {
		return
	}
	// SLOWLOG and EXEC never log themselves, matching the CMD_SKIP_SLOWLOG flag in
	// Redis. The commands run inside an EXEC still log on their own.
	if len(argv) > 0 {
		switch strings.ToLower(string(argv[0])) {
		case "slowlog", "exec":
			return
		}
	}
	maxLen := d.confInt("slowlog-max-len", 128)
	if maxLen <= 0 {
		return
	}
	entry := slowlogEntry{
		id:    d.slowlog.nextID.Add(1) - 1,
		ts:    time.Now().Unix(),
		durUs: durUs,
		args:  slowlogArgs(argv),
		addr:  c.RemoteAddr(),
		name:  c.Name(),
	}
	d.slowlog.mu.Lock()
	d.slowlog.entries = append(d.slowlog.entries, entry)
	if int64(len(d.slowlog.entries)) > maxLen {
		d.slowlog.entries = d.slowlog.entries[int64(len(d.slowlog.entries))-maxLen:]
	}
	d.slowlog.mu.Unlock()
}

// connInfo is the slice of the connection the slow log records. runCommand passes
// the live *networking.Conn; it is an interface so the recording logic stays
// testable without a socket.
type connInfo interface {
	RemoteAddr() string
	Name() string
}

// slowlogArgs renders an argv into the truncated string form the slow log keeps.
// Long arguments are cut at slowlogArgMax bytes and a long argument list is folded
// after slowlogArgCount entries.
func slowlogArgs(argv [][]byte) []string {
	n := len(argv)
	extra := 0
	if n > slowlogArgCount {
		n = slowlogArgCount - 1
		extra = len(argv) - n
	}
	out := make([]string, 0, n+1)
	for _, a := range argv[:n] {
		if len(a) > slowlogArgMax {
			out = append(out, string(a[:slowlogArgMax])+"...")
		} else {
			out = append(out, string(a))
		}
	}
	if extra > 0 {
		out = append(out, "... ("+strconv.Itoa(extra)+" more arguments)")
	}
	return out
}

// slowlogGet returns the most recent entries newest first, capped at count. A
// negative count returns every entry.
func (d *Dispatcher) slowlogGet(count int) []slowlogEntry {
	d.slowlog.mu.Lock()
	defer d.slowlog.mu.Unlock()
	total := len(d.slowlog.entries)
	if count < 0 || count > total {
		count = total
	}
	out := make([]slowlogEntry, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, d.slowlog.entries[total-1-i])
	}
	return out
}

// slowlogLen returns the number of entries currently held.
func (d *Dispatcher) slowlogLen() int {
	d.slowlog.mu.Lock()
	defer d.slowlog.mu.Unlock()
	return len(d.slowlog.entries)
}

// slowlogReset discards every entry. The id counter keeps running so ids stay
// unique across a reset, the same as Redis.
func (d *Dispatcher) slowlogReset() {
	d.slowlog.mu.Lock()
	d.slowlog.entries = nil
	d.slowlog.mu.Unlock()
}
