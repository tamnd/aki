package shard

import "sync/atomic"

// Keyspace notifications (spec 2064/f3/11, the M11 command-closure milestone;
// redis's notify-keyspace-events). A write publishes two pub/sub messages when
// its class is enabled: on __keyspace@0__:<key> the event name, and on
// __keyevent@0__:<event> the key. A client subscribes to either channel family
// to learn a key changed without polling, the cache-invalidation surface that is
// the whole point of the feature.
//
// The seam. The class-flag bitmask is a process atomic CONFIG SET writes
// (dispatch/config.go, through SetNotifyFlags) and the emit helper reads on the
// owner, the same shape the evictor's maxmemory atomics have. The actual publish
// goes through a per-worker hook (Runtime.UsePublisher) the server layer wires to
// its pub/sub registry, because the registry is per-Server state the shard cannot
// import; this mirrors UseEvictor and UseDemoter. All of the bit logic (parse,
// render, the class gate, the channel formatting) lives here so every keyspace
// shares one path and pays one atomic load when notifications are off, the default.
//
// This slice's coverage, stated honestly. The generic class fires from the
// centralized cross-type commands in dispatch that already own one clear mutation
// site: del (DEL), persist (PERSIST), rename_from and rename_to (RENAME/RENAMENX),
// copy_to (COPY), and restore (RESTORE); the evicted event fires from the maxmemory
// evictor. The per-type data events ($ string, l list, s set, h hash, z zset, t
// stream), the new-key (n) and key-miss (m) events, the expire event (EXPIRE and
// its siblings, whose reply is written inside the per-type Expire handler so the
// mutation bool is not available at the centralized route), and the expired event
// (both the lazy-expiry read path and the active-expiry reaper) are the remaining
// surface, each needing per-handler mutation-precise emission across dozens of
// sites; they are a documented follow-up. The config parses and round-trips every
// class letter regardless, so a client can subscribe to a class this slice does
// not yet emit without CONFIG erroring.

// The class-flag bits. K and E pick the channel families; the rest are event
// classes. The letters match redis's notify-keyspace-events alphabet so a config
// string round-trips.
const (
	NotifyKeyspace uint32 = 1 << iota // K, the __keyspace@<db>__ channel
	NotifyKeyevent                    // E, the __keyevent@<db>__ channel
	NotifyGeneric                     // g, DEL/EXPIRE/RENAME/PERSIST/...
	NotifyString                      // $, string commands
	NotifyList                        // l, list commands
	NotifySet                         // s, set commands
	NotifyHash                        // h, hash commands
	NotifyZset                        // z, sorted-set commands
	NotifyExpired                     // x, a key expiring
	NotifyEvicted                     // e, a key evicted by maxmemory
	NotifyStream                      // t, stream commands
	NotifyModule                      // d, module key type events
	NotifyNew                         // n, a new key created
	NotifyKeymiss                     // m, a read that missed
)

// NotifyAll is redis's 'A' alias: every event class except the channel-family
// bits (K, E) and the two special classes new (n) and key-miss (m), matching
// redis's NOTIFY_ALL so a "KEA" config normalizes to "AKE".
const NotifyAll = NotifyGeneric | NotifyString | NotifyList | NotifySet |
	NotifyHash | NotifyZset | NotifyExpired | NotifyEvicted | NotifyStream | NotifyModule

// notifyFlags is the live class-flag bitmask, zero (every class off) by default so
// a server that never sets notify-keyspace-events pays only one atomic load per
// candidate event and publishes nothing. CONFIG SET writes it through
// SetNotifyFlags; the emit helper reads it on the owner.
var notifyFlags atomic.Uint32

// SetNotifyFlags installs the live keyspace-notification class mask, the value
// dispatch's CONFIG SET arm computed from the notify-keyspace-events flag string.
// Process-global like the active-expiry toggle, since the config is server-wide.
func SetNotifyFlags(flags uint32) { notifyFlags.Store(flags) }

// LoadNotifyFlags reports the live class mask, for a caller that wants to gate a
// batch of events on one load rather than paying the load per event.
func LoadNotifyFlags() uint32 { return notifyFlags.Load() }

// ParseNotifyFlags reads a notify-keyspace-events flag string into the class mask.
// Each letter sets its class; 'A' sets every class in NotifyAll. ok is false on an
// unknown letter, so CONFIG SET rejects a malformed value rather than storing a
// mask that silently drops a class the client asked for. The empty string is the
// disabled mask, zero, ok true.
func ParseNotifyFlags(s string) (uint32, bool) {
	var flags uint32
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 'K':
			flags |= NotifyKeyspace
		case 'E':
			flags |= NotifyKeyevent
		case 'g':
			flags |= NotifyGeneric
		case '$':
			flags |= NotifyString
		case 'l':
			flags |= NotifyList
		case 's':
			flags |= NotifySet
		case 'h':
			flags |= NotifyHash
		case 'z':
			flags |= NotifyZset
		case 'x':
			flags |= NotifyExpired
		case 'e':
			flags |= NotifyEvicted
		case 't':
			flags |= NotifyStream
		case 'd':
			flags |= NotifyModule
		case 'n':
			flags |= NotifyNew
		case 'm':
			flags |= NotifyKeymiss
		case 'A':
			flags |= NotifyAll
		default:
			return 0, false
		}
	}
	return flags, true
}

// NotifyFlagsString renders the class mask back to the canonical flag string
// redis's CONFIG GET returns: the 'A' alias when every NotifyAll class is set,
// else each class letter in redis's order, then K and E. This is the value stored
// so a later GET reads it back the way redis does.
func NotifyFlagsString(flags uint32) string {
	var b []byte
	if flags&NotifyAll == NotifyAll {
		b = append(b, 'A')
	} else {
		if flags&NotifyGeneric != 0 {
			b = append(b, 'g')
		}
		if flags&NotifyString != 0 {
			b = append(b, '$')
		}
		if flags&NotifyList != 0 {
			b = append(b, 'l')
		}
		if flags&NotifySet != 0 {
			b = append(b, 's')
		}
		if flags&NotifyHash != 0 {
			b = append(b, 'h')
		}
		if flags&NotifyZset != 0 {
			b = append(b, 'z')
		}
		if flags&NotifyExpired != 0 {
			b = append(b, 'x')
		}
		if flags&NotifyEvicted != 0 {
			b = append(b, 'e')
		}
		if flags&NotifyStream != 0 {
			b = append(b, 't')
		}
		if flags&NotifyModule != 0 {
			b = append(b, 'd')
		}
	}
	// n and m are not in the 'A' alias, so they always render explicitly.
	if flags&NotifyNew != 0 {
		b = append(b, 'n')
	}
	if flags&NotifyKeymiss != 0 {
		b = append(b, 'm')
	}
	if flags&NotifyKeyspace != 0 {
		b = append(b, 'K')
	}
	if flags&NotifyKeyevent != 0 {
		b = append(b, 'E')
	}
	return string(b)
}

// UsePublisher registers the pub/sub publish hook the keyspace-notification
// emitter calls. The server layer passes a closure over its pub/sub registry
// (s.pubsub.publish), which the shard cannot import; a runtime with no pub/sub
// leaves the hook nil and NotifyKeyspaceEvent publishes nothing. Fixed before
// Start like Use and UseEvictor, so the owner reads it with no synchronization.
// The hook delivers to subscribers through the connection out-of-band path, which
// is safe from the owner goroutine.
func (r *Runtime) UsePublisher(fn func(channel string, message []byte)) {
	if r.started {
		panic("shard: UsePublisher after Start")
	}
	for _, w := range r.workers {
		w.publisher = fn
	}
}

// NotifyKeyspaceEvent publishes a keyspace notification for a write, when its
// class is enabled. class is the event's class bit (NotifyString, NotifyGeneric,
// ...); event is redis's short event name ("set", "del", "expired"); key is the
// key it happened to. It gates on the live mask first, so a disabled server (the
// default) returns after one atomic load, and on the publisher hook, so a runtime
// with no pub/sub returns at once. When enabled it publishes the event name on the
// key's __keyspace@0__ channel if K is on and the key on the event's __keyevent@0__
// channel if E is on. Owner goroutine only; the publish is out-of-band-safe.
func (cx *Ctx) NotifyKeyspaceEvent(class uint32, event string, key []byte) {
	flags := notifyFlags.Load()
	if flags&class == 0 {
		return
	}
	if flags&(NotifyKeyspace|NotifyKeyevent) == 0 {
		return
	}
	if cx.w == nil || cx.w.publisher == nil {
		return
	}
	if flags&NotifyKeyspace != 0 {
		// __keyspace@0__:<key> carries the event name.
		ch := make([]byte, 0, len(keyspacePrefix)+len(key))
		ch = append(ch, keyspacePrefix...)
		ch = append(ch, key...)
		cx.w.publisher(string(ch), []byte(event))
	}
	if flags&NotifyKeyevent != 0 {
		// __keyevent@0__:<event> carries the key.
		ch := make([]byte, 0, len(keyeventPrefix)+len(event))
		ch = append(ch, keyeventPrefix...)
		ch = append(ch, event...)
		cx.w.publisher(ch2s(ch), append([]byte(nil), key...))
	}
}

// The single-db channel prefixes. f3 runs one logical db, so the db index is
// always 0, matching what a default redis reports.
const (
	keyspacePrefix = "__keyspace@0__:"
	keyeventPrefix = "__keyevent@0__:"
)

// ch2s converts a freshly-built channel slice to a string. The slice is owned by
// this call and never mutated after, so the conversion copy is the only aliasing
// concern and Go's []byte->string handles it.
func ch2s(b []byte) string { return string(b) }
