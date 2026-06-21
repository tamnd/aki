package command

import (
	"strconv"
	"sync/atomic"

	"github.com/tamnd/aki/networking"
)

// Keyspace notification category bits. notify-keyspace-events parses its flag
// string into an OR of these, and each command that mutates a key fires with the
// bit its event belongs to. The values match the layout in doc 14 §16.5.
const (
	notifyKeyspace = uint32(1 << 0)  // K, the __keyspace@<db>__ channel form
	notifyKeyevent = uint32(1 << 1)  // E, the __keyevent@<db>__ channel form
	notifyGeneric  = uint32(1 << 2)  // g, cross-type commands like DEL and EXPIRE
	notifyString   = uint32(1 << 3)  // $, string commands
	notifyList     = uint32(1 << 4)  // l, list commands
	notifySet      = uint32(1 << 5)  // s, set commands
	notifyHash     = uint32(1 << 6)  // h, hash commands
	notifyZset     = uint32(1 << 7)  // z, sorted set commands
	notifyExpired  = uint32(1 << 8)  // x, key expiry
	notifyEvicted  = uint32(1 << 9)  // e, key eviction
	notifyNewKey   = uint32(1 << 10) // n, new key creation
	notifyStream   = uint32(1 << 11) // t, stream commands
	notifyModule   = uint32(1 << 12) // d, module key type events
	notifyKeyMiss  = uint32(1 << 13) // m, key miss on read

	// notifyAll is the A flag: every standard event category. It does not cover
	// new, key-miss or module, which must be named on their own.
	notifyAll = notifyGeneric | notifyString | notifyList | notifySet |
		notifyHash | notifyZset | notifyExpired | notifyEvicted | notifyStream
)

// parseNotifyFlags reads a notify-keyspace-events flag string into a bitmask. It
// reports false on any character that is not a known flag, which CONFIG SET turns
// into a parse error.
func parseNotifyFlags(s string) (uint32, bool) {
	var flags uint32
	for _, ch := range s {
		switch ch {
		case 'K':
			flags |= notifyKeyspace
		case 'E':
			flags |= notifyKeyevent
		case 'g':
			flags |= notifyGeneric
		case '$':
			flags |= notifyString
		case 'l':
			flags |= notifyList
		case 's':
			flags |= notifySet
		case 'h':
			flags |= notifyHash
		case 'z':
			flags |= notifyZset
		case 'x':
			flags |= notifyExpired
		case 'e':
			flags |= notifyEvicted
		case 'n':
			flags |= notifyNewKey
		case 't':
			flags |= notifyStream
		case 'd':
			flags |= notifyModule
		case 'm':
			flags |= notifyKeyMiss
		case 'A':
			flags |= notifyAll
		default:
			return 0, false
		}
	}
	return flags, true
}

// canonicalNotifyFlags renders a bitmask back into the canonical flag string, the
// form CONFIG GET reports. The A shorthand stands in for the full standard set so
// a value round trips: "KEA" stored, "AKE" read back.
func canonicalNotifyFlags(flags uint32) string {
	var b []byte
	if flags&notifyAll == notifyAll {
		b = append(b, 'A')
	} else {
		if flags&notifyGeneric != 0 {
			b = append(b, 'g')
		}
		if flags&notifyString != 0 {
			b = append(b, '$')
		}
		if flags&notifyList != 0 {
			b = append(b, 'l')
		}
		if flags&notifySet != 0 {
			b = append(b, 's')
		}
		if flags&notifyHash != 0 {
			b = append(b, 'h')
		}
		if flags&notifyZset != 0 {
			b = append(b, 'z')
		}
		if flags&notifyExpired != 0 {
			b = append(b, 'x')
		}
		if flags&notifyEvicted != 0 {
			b = append(b, 'e')
		}
		if flags&notifyStream != 0 {
			b = append(b, 't')
		}
	}
	if flags&notifyKeyMiss != 0 {
		b = append(b, 'm')
	}
	if flags&notifyNewKey != 0 {
		b = append(b, 'n')
	}
	if flags&notifyModule != 0 {
		b = append(b, 'd')
	}
	if flags&notifyKeyspace != 0 {
		b = append(b, 'K')
	}
	if flags&notifyKeyevent != 0 {
		b = append(b, 'E')
	}
	return string(b)
}

// notifyKeyspaceEvent fires the keyspace and keyevent notifications for an event
// on key in database dbIndex. eventType is the category bit the event belongs to.
// The call returns at once when notifications are off or the category is not
// enabled, so a server with notifications disabled pays one atomic load per write.
func (d *Dispatcher) notifyKeyspaceEvent(dbIndex int, eventType uint32, event, key string) {
	flags := atomic.LoadUint32(&d.notifyFlags)
	if flags&eventType == 0 {
		return
	}
	dbStr := strconv.Itoa(dbIndex)
	// Key-miss has no meaningful keyspace channel since the key does not exist, so
	// it fires the keyevent form only.
	if flags&notifyKeyspace != 0 && eventType != notifyKeyMiss {
		d.publishTo("__keyspace@"+dbStr+"__:"+key, event)
	}
	if flags&notifyKeyevent != 0 {
		d.publishTo("__keyevent@"+dbStr+"__:"+event, key)
	}
}

// notify fires a keyspace event for key in the connection's current database. The
// command handlers call it after a mutation commits.
func (ctx *Ctx) notify(eventType uint32, event string, key []byte) {
	ctx.d.notifyKeyspaceEvent(ctx.Conn.DB(), eventType, event, string(key))
}

// publishTo delivers a message to channel subscribers and to every matching
// pattern subscriber, returning the delivery count. PUBLISH and the keyspace
// notification path share it.
func (d *Dispatcher) publishTo(channel, msg string) int {
	chB := []byte(channel)
	msgB := []byte(msg)

	type target struct {
		conn    *networking.Conn
		pattern []byte
	}
	var targets []target

	d.ps.mu.RLock()
	for _, conn := range d.ps.channels[channel] {
		targets = append(targets, target{conn: conn})
	}
	for pat, conns := range d.ps.patterns {
		if !stringMatch([]byte(pat), chB, false) {
			continue
		}
		pb := []byte(pat)
		for _, conn := range conns {
			targets = append(targets, target{conn: conn, pattern: pb})
		}
	}
	d.ps.mu.RUnlock()

	count := 0
	for _, t := range targets {
		var frame []byte
		if t.pattern != nil {
			frame = framePMessage(t.conn.Proto(), t.pattern, chB, msgB)
		} else {
			frame = frameMessage(t.conn.Proto(), "message", chB, msgB)
		}
		if err := t.conn.Deliver(frame); err == nil {
			count++
		}
	}
	return count
}
