package mesh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// DefaultCallTimeout bounds any mesh call whose context carries no
// deadline. P-I4 makes this a correctness posture, not a tuning knob:
// every verb has a fallback, so a call that cannot complete quickly
// should fail into it rather than wait.
const DefaultCallTimeout = time.Second

// VerbError is a refusal the peer answered on a healthy connection, as
// opposed to a transport failure: "repl not enabled" while the standby
// seam is empty, "unknown mesh verb" across versions. Fallback logic
// treats both the same way (fall back), but the taxonomy differs.
type VerbError struct{ Msg string }

func (e *VerbError) Error() string { return "mesh: " + e.Msg }

// PeerConfig wires one outbound peer.
type PeerConfig struct {
	Addr   string
	Secret string
	// Dial overrides the transport; nil dials TCP to Addr. A tls.Dialer
	// wrapper lands here when config asks for TLS.
	Dial func(ctx context.Context) (net.Conn, error)
	// CallTimeout replaces DefaultCallTimeout when positive.
	CallTimeout time.Duration
}

type callReply struct {
	out [][]byte
	err error
}

// Peer is one node's multiplexed connection to one other node: dialed
// lazily on the first call, re-dialed on the next call after any
// failure, never retried inside a call. Calls from any goroutine
// interleave on the one connection by id.
type Peer struct {
	cfg PeerConfig

	mu      sync.Mutex
	conn    net.Conn
	wmu     sync.Mutex
	pending map[uint64]chan callReply
	nextID  uint64
	closed  bool
}

// NewPeer builds a peer; no connection happens until the first call.
func NewPeer(cfg PeerConfig) (*Peer, error) {
	if cfg.Secret == "" {
		return nil, fmt.Errorf("mesh: peer needs a shared secret")
	}
	if cfg.Addr == "" && cfg.Dial == nil {
		return nil, fmt.Errorf("mesh: peer needs an address or a dialer")
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = DefaultCallTimeout
	}
	return &Peer{cfg: cfg, pending: make(map[uint64]chan callReply)}, nil
}

// Close drops the connection and fails every waiting call.
func (p *Peer) Close() {
	p.mu.Lock()
	p.closed = true
	c := p.conn
	p.conn = nil
	waiters := p.takeWaitersLocked()
	p.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
	for _, ch := range waiters {
		ch <- callReply{err: errors.New("mesh: peer closed")}
	}
}

// Int carries one intent leg and returns the peer's payload.
func (p *Peer) Int(ctx context.Context, payload [][]byte) ([][]byte, error) {
	return p.Call(ctx, VerbInt, payload)
}

// Wake nudges the peer's blocking machinery for a group's key.
func (p *Peer) Wake(ctx context.Context, group uint16, key []byte) error {
	_, err := p.Call(ctx, VerbWake, [][]byte{[]byte(strconv.FormatUint(uint64(group), 10)), key})
	return err
}

// Repl ships one hot-standby frame batch; until the doc 04 section 7
// standby lands every peer refuses it with a VerbError.
func (p *Peer) Repl(ctx context.Context, frames [][]byte) error {
	_, err := p.Call(ctx, VerbRepl, frames)
	return err
}

// Hint nudges the peer to read a log domain's chain now.
func (p *Peer) Hint(ctx context.Context, dd uint8) error {
	_, err := p.Call(ctx, VerbHint, [][]byte{[]byte(strconv.FormatUint(uint64(dd), 10))})
	return err
}

// Call runs one verb round trip. A context without a deadline gets the
// peer's call timeout; any transport failure drops the connection so the
// next call re-dials.
func (p *Peer) Call(ctx context.Context, verb string, args [][]byte) ([][]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.CallTimeout)
		defer cancel()
	}
	c, err := p.ensure(ctx)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.conn != c {
		p.mu.Unlock()
		return nil, errors.New("mesh: connection dropped")
	}
	p.nextID++
	id := p.nextID
	ch := make(chan callReply, 1)
	p.pending[id] = ch
	p.mu.Unlock()

	frame := appendFrame(nil, append([][]byte{
		[]byte(strconv.FormatUint(id, 10)), []byte(verb),
	}, args...)...)
	if err := p.write(ctx, c, frame); err != nil {
		p.fail(c, err)
		return nil, err
	}
	select {
	case r := <-ch:
		return r.out, r.err
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (p *Peer) write(ctx context.Context, c net.Conn, frame []byte) error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	if d, ok := ctx.Deadline(); ok {
		_ = c.SetWriteDeadline(d)
		defer func() { _ = c.SetWriteDeadline(time.Time{}) }()
	}
	_, err := c.Write(frame)
	return err
}

// ensure returns the live authenticated connection, dialing one if none
// exists. Dial and auth happen outside the peer mutex; a racing second
// dial loses and its connection closes.
func (p *Peer) ensure(ctx context.Context) (net.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("mesh: peer closed")
	}
	if p.conn != nil {
		c := p.conn
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()

	c, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}
	if err := p.auth(ctx, c); err != nil {
		_ = c.Close()
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("mesh: peer closed")
	}
	if p.conn != nil {
		got := p.conn
		p.mu.Unlock()
		_ = c.Close()
		return got, nil
	}
	p.conn = c
	p.mu.Unlock()
	go p.readLoop(c)
	return c, nil
}

func (p *Peer) dial(ctx context.Context) (net.Conn, error) {
	if p.cfg.Dial != nil {
		return p.cfg.Dial(ctx)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", p.cfg.Addr)
}

// auth runs the first-frame handshake synchronously, before the read
// loop exists, under the context's deadline.
func (p *Peer) auth(ctx context.Context, c net.Conn) error {
	if d, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(d)
		defer func() { _ = c.SetDeadline(time.Time{}) }()
	}
	frame := appendFrame(nil, []byte("0"), []byte(VerbAuth), []byte(p.cfg.Secret), []byte("node"))
	if _, err := c.Write(frame); err != nil {
		return err
	}
	var fc frameConn
	tmp := make([]byte, readChunk)
	for {
		n, rerr := c.Read(tmp)
		var got [][]byte
		if n > 0 {
			if err := fc.feed(tmp[:n], func(args [][]byte) error {
				got = args
				return nil
			}); err != nil {
				return err
			}
			if got != nil {
				if len(got) >= 2 && string(got[1]) == "ok" {
					return nil
				}
				return errors.New("mesh: auth refused")
			}
		}
		if rerr != nil {
			return rerr
		}
	}
}

// readLoop routes replies to waiting calls until the connection dies,
// then fails everything left.
func (p *Peer) readLoop(c net.Conn) {
	var fc frameConn
	tmp := make([]byte, readChunk)
	for {
		n, rerr := c.Read(tmp)
		if n > 0 {
			if err := fc.feed(tmp[:n], p.deliver); err != nil {
				p.fail(c, err)
				return
			}
		}
		if rerr != nil {
			p.fail(c, rerr)
			return
		}
	}
}

func (p *Peer) deliver(args [][]byte) error {
	if len(args) < 2 {
		return errors.New("mesh: short reply")
	}
	id, err := strconv.ParseUint(string(args[0]), 10, 64)
	if err != nil {
		return errors.New("mesh: bad reply id")
	}
	p.mu.Lock()
	ch, ok := p.pending[id]
	delete(p.pending, id)
	p.mu.Unlock()
	if !ok {
		// The call gave up on its deadline already; the late reply drops.
		return nil
	}
	switch string(args[1]) {
	case "ok":
		ch <- callReply{out: args[2:]}
	case "err":
		msg := ""
		if len(args) > 2 {
			msg = string(args[2])
		}
		ch <- callReply{err: &VerbError{Msg: msg}}
	default:
		ch <- callReply{err: errors.New("mesh: bad reply status")}
	}
	return nil
}

// fail drops the connection if it is still current and fails all waiters.
func (p *Peer) fail(c net.Conn, err error) {
	p.mu.Lock()
	if p.conn != c {
		p.mu.Unlock()
		_ = c.Close()
		return
	}
	p.conn = nil
	waiters := p.takeWaitersLocked()
	p.mu.Unlock()
	_ = c.Close()
	for _, ch := range waiters {
		ch <- callReply{err: fmt.Errorf("mesh: connection failed: %w", err)}
	}
}

func (p *Peer) takeWaitersLocked() []chan callReply {
	out := make([]chan callReply, 0, len(p.pending))
	for id, ch := range p.pending {
		delete(p.pending, id)
		out = append(out, ch)
	}
	return out
}
