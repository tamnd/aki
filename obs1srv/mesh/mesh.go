// Package mesh is the fleet's internal fabric (spec 2064/obs1 doc 07
// section 5): every node holds one multiplexed RESP-framed connection to
// every other, authenticated by a shared secret from settings. The verbs
// this slice carries are M.INT (cross-node intent legs), M.WAKE
// (blocking-command wakeups), M.REPL (the doc 04 section 7 hot-standby
// seam, wire shape only), and M.HINT (read-the-chain-now nudges, never
// authority). M.FWD arrives with the proxy listener at O6b and M.PUB
// with pubsub.
//
// P-I4 is the design law here: no mesh verb is load-bearing for
// correctness, every one has a chain-or-redirect fallback, so every
// caller-side path is built to fail fast and cleanly rather than to
// retry hard. A call against a dead peer returns an error inside its
// deadline and the caller falls back; nothing in this package blocks
// past DefaultCallTimeout unless the caller asked for longer.
//
// Framing: every frame in both directions is one RESP array of bulk
// strings, parsed by the obs1srv/resp parser. A request is
// [id, verb, args...]; a reply is [id, "ok", results...] or
// [id, "err", message]. The id lets replies interleave, which is the
// whole of the multiplexing: a slow verb never head-of-line blocks the
// connection because the listener dispatches each request on its own
// goroutine and the writer is serialized per frame. The first frame on
// a connection must be [0, "M.AUTH", secret, nodeid]; everything before
// a good auth is refused and the connection closed.
//
// TLS per config is a wrap seam, not machinery here: the listener side
// accepts any net.Listener and the peer side any dial function, so a
// tls.Listener and a tls.Dialer drop in without this package knowing.
package mesh

import (
	"fmt"

	"github.com/tamnd/aki/obs1srv/resp"
)

// The verb names, the aki-internal command namespace.
const (
	VerbAuth = "M.AUTH"
	VerbInt  = "M.INT"
	VerbWake = "M.WAKE"
	VerbRepl = "M.REPL"
	VerbHint = "M.HINT"
)

const (
	// maxFrame bounds one mesh frame's parse buffer; a peer that streams
	// past it is broken or hostile and the connection drops. REPL frame
	// batches size themselves under it.
	maxFrame = 8 << 20

	// readChunk is the transport read granularity.
	readChunk = 64 << 10
)

// appendFrame encodes one frame: an array of bulk strings.
func appendFrame(dst []byte, parts ...[]byte) []byte {
	dst = resp.AppendArrayHeader(dst, len(parts))
	for _, p := range parts {
		dst = resp.AppendBulk(dst, p)
	}
	return dst
}

// frameConn is the shared read half: accumulate bytes, hand each parsed
// frame to fn as copies the caller may retain, since the parse buffer
// compacts underneath the views.
type frameConn struct {
	buf []byte
	par resp.Parser
}

func (f *frameConn) feed(b []byte, fn func(args [][]byte) error) error {
	f.buf = append(f.buf, b...)
	for {
		args, n, st := f.par.Next(f.buf)
		switch st {
		case resp.OK:
			owned := make([][]byte, len(args))
			for i, a := range args {
				owned[i] = append([]byte(nil), a...)
			}
			f.buf = append(f.buf[:0], f.buf[n:]...)
			if len(owned) == 0 {
				continue
			}
			if err := fn(owned); err != nil {
				return err
			}
		case resp.NeedMore:
			if len(f.buf) > maxFrame {
				return fmt.Errorf("mesh: frame exceeds %d bytes", maxFrame)
			}
			return nil
		default:
			return fmt.Errorf("mesh: bad frame: %s", f.par.LastError())
		}
	}
}
