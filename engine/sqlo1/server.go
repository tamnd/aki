package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// serverMemCap is the hot-tier budget the server hands ComputeBudget
// until the config surface lands (doc 04 section 15's --memory-cap).
const serverMemCap = 64 << 20

// Server is the RESP endpoint over the shard runtime: one Tiered plus
// the type layers, one goroutine per connection, replies batched per
// read so pipelining works. The mutex is the single-owner discipline
// the runtime assumes (R1), so command execution is serial; the shard
// fan-out that makes that scale is the doc 04 section 2 server work.
type Server struct {
	t  *Tiered
	s  *Str
	h  *Hash
	se *Set
	z  *ZSet
	l  *List
	x  *Stream

	mu sync.Mutex // serializes command execution against the runtime

	// now is the clock, swappable by tests that exercise expiry.
	now func() int64 // wall milliseconds

	// old carries a reply value across the mutation that would recycle
	// its arena bytes (SET GET, GETDEL, GETEX).
	old []byte

	// mkeys and mvals split MSET and MSETNX argument pairs.
	mkeys [][]byte
	mvals [][]byte

	// bfOps holds one BITFIELD command's parsed subcommands.
	bfOps []BitfieldOp

	// scanBuf stages one HSCAN step's elements: MATCH decides the
	// element count only after the walk, so the inner array header
	// cannot go down first the way HGETALL's does.
	scanBuf []byte

	// ttlBuf holds one HEXPIRE-family command's per-field codes.
	ttlBuf []int64

	// zscores holds one ZADD command's parsed scores: all of them
	// parse before any write, so a bad float later in the list cannot
	// leave a half-applied command.
	zscores []float64

	// REV range scratch: emitted members alias run reads that die as
	// the walk advances, so a reversed reply buffers copies here and
	// replays them backward. zlexbuf builds a lex bound's successor
	// member (member plus a low byte).
	zrarena []byte
	zrpairs []zbuildPair
	zlexbuf []byte

	// Geo search scratch: the cover walk's matches copy here (walk
	// bytes alias run reads and die as the cells advance) before the
	// sort, trim, and reply or store. zrpairs doubles as the store
	// form's build pair scratch.
	geoArena []byte
	geoHits  []geoHit

	// Blocking pop machinery, zpopcmd.go: waiters sleep on zbcond
	// (over mu), every dispatch broadcasts on its way out, and zbwait
	// holds each key's FIFO ticket queue so the longest-blocked
	// client is served first. zbnext mints the tickets.
	zbcond *sync.Cond
	zbnext uint64
	zbwait map[string][]uint64
}

// splitPairs fills mkeys and mvals from an even-length key-value
// argument run; the caller has already checked the arity.
func (s *Server) splitPairs(pairs [][]byte) {
	s.mkeys, s.mvals = s.mkeys[:0], s.mvals[:0]
	for i := 0; i < len(pairs); i += 2 {
		s.mkeys = append(s.mkeys, pairs[i])
		s.mvals = append(s.mvals, pairs[i+1])
	}
}

// NewServer wires the command surface over st. The store must expose
// the Minter capability: the string ladder cannot build ropes without
// durable rooth leases.
func NewServer(st Store) (*Server, error) {
	srv := &Server{now: func() int64 { return time.Now().UnixMilli() }}
	srv.zbcond = sync.NewCond(&srv.mu)
	srv.zbwait = map[string][]uint64{}
	t := NewTiered(st, TieredConfig{
		Budget: ComputeBudget(serverMemCap, 1),
		NowMs:  func() int64 { return srv.now() },
	})
	str, err := NewStr(t, StrConfig{})
	if err != nil {
		return nil, err
	}
	hash, err := NewHash(t, HashConfig{})
	if err != nil {
		return nil, err
	}
	set, err := NewSet(t, HashConfig{})
	if err != nil {
		return nil, err
	}
	zset, err := NewZSet(t, HashConfig{})
	if err != nil {
		return nil, err
	}
	list, err := NewList(t, ListConfig{})
	if err != nil {
		return nil, err
	}
	stream, err := NewStream(t, StreamConfig{})
	if err != nil {
		return nil, err
	}
	srv.t, srv.s, srv.h, srv.se, srv.z = t, str, hash, set, zset
	srv.l, srv.x = list, stream
	return srv, nil
}

// Serve accepts connections until the listener closes. A once-a-second
// tick runs the runtime's timer maintenance (drain quanta, checkpoint,
// compaction steps) between commands; a tick error is not fatal here
// because the same store error surfaces on the next command.
func (s *Server) Serve(l net.Listener) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				s.mu.Lock()
				s.t.Tick(context.Background())
				s.mu.Unlock()
			}
		}
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(c)
	}
}

// Flush drains the hot tier to the store until nothing is dirty. The
// durable seam sits at the store: an acked write lives only in the hot
// tier until a drain carries it down, so an embedder that closes the
// store without this call keeps only the last drained prefix, exactly
// the crash contract. Clean shutdown is listener close, Serve return,
// Flush, then store close.
func (s *Server) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.t.Flush(ctx)
}

func (s *Server) handleConn(c net.Conn) {
	defer c.Close()
	buf := make([]byte, 0, 16<<10)
	reply := make([]byte, 0, 4<<10)
	args := make([][]byte, 0, 8)
	for {
		// Parse everything buffered, then write all replies in one call, so
		// a pipelined burst costs one read and one write.
		reply = reply[:0]
		consumed := 0
		for {
			var n int
			var err error
			args, n, err = ParseCommand(buf[consumed:], args[:0])
			if errors.Is(err, ErrIncomplete) {
				break
			}
			if pe, ok := errors.AsType[*ProtoError](err); ok {
				reply = AppendError(reply, pe.Error())
				c.Write(reply)
				return
			}
			consumed += n
			if len(args) == 0 {
				continue
			}
			reply = s.dispatch(reply, args)
		}
		if len(reply) > 0 {
			if _, err := c.Write(reply); err != nil {
				return
			}
		}
		buf = append(buf[:0], buf[consumed:]...)

		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := c.Read(buf[len(buf):cap(buf)])
		if err != nil {
			return
		}
		buf = buf[:len(buf)+n]
	}
}

func (s *Server) dispatch(reply []byte, args [][]byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Any command may have produced the data a blocked pop waits on,
	// so every dispatch wakes the waiters on its way out. The deferred
	// broadcast runs before the unlock (LIFO), which sync.Cond allows.
	defer s.zbcond.Broadcast()

	// Re-stamping the exact clock per command is what makes lazy expiry
	// millisecond-exact at command time (doc 11 E-I1); every expiry this
	// dispatch computes or compares uses the same reading.
	now := s.t.Now()
	ctx := context.Background()

	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "PING":
		switch len(args) {
		case 1:
			return AppendSimple(reply, "PONG")
		case 2:
			return AppendBulk(reply, args[1])
		}
		return arityErr(reply, cmd)
	case "ECHO":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		return AppendBulk(reply, args[1])
	case "INFO":
		// One section for now, whatever section the client asked for.
		// The io_backend line is the doc 13 provenance requirement: a
		// gate run must know which backend ran, and the fallback is
		// silent so this is the only place the fact surfaces.
		ss := s.t.StoreStats()
		ts := s.t.Stats()
		backend := ss.IOBackend
		if backend == "" {
			backend = "none"
		}
		info := fmt.Sprintf("# sqlo1\r\nio_backend:%s\r\nkeys:%d\r\ndisk_bytes:%d\r\nhigh_water:%d\r\nhot_keys:%d\r\ndirty_bytes:%d\r\n",
			backend, ss.Keys, ss.DiskBytes, ss.HighWater, ts.HotKeys, ts.DirtyBytes)
		return AppendBulk(reply, []byte(info))
	case "SET":
		return s.setCmd(ctx, reply, args, now)
	case "SETNX":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		exists, _, err := s.s.Entry(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if exists {
			return AppendInt(reply, 0)
		}
		if err := s.s.Set(ctx, args[1], args[2]); err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, 1)
	case "SETEX", "PSETEX":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		n, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		unit := int64(1000)
		if cmd == "PSETEX" {
			unit = 1
		}
		at, ok := expireFrom(now, n, unit)
		if n <= 0 || !ok {
			return invalidExpire(reply, cmd)
		}
		if err := s.s.Set(ctx, args[1], args[3]); err != nil {
			return storeErr(reply, err)
		}
		if _, err := s.s.ExpireAt(ctx, args[1], at); err != nil {
			return storeErr(reply, err)
		}
		return AppendSimple(reply, "OK")
	case "GET":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		v, ok, err := s.s.Get(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, v)
	case "GETDEL":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		v, ok, err := s.s.Get(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		s.old = append(s.old[:0], v...)
		if _, err := s.s.Del(ctx, args[1]); err != nil {
			return storeErr(reply, err)
		}
		return AppendBulk(reply, s.old)
	case "GETEX":
		return s.getexCmd(ctx, reply, args, now)
	case "STRLEN":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		n, _, err := s.s.Strlen(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "SUBSTR", "GETRANGE":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		start, err1 := strconv.ParseInt(string(args[2]), 10, 64)
		end, err2 := strconv.ParseInt(string(args[3]), 10, 64)
		if err1 != nil || err2 != nil {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		v, err := s.s.Range(ctx, args[1], start, end)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendBulk(reply, v)
	case "APPEND":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		n, err := s.s.Append(ctx, args[1], args[2])
		if err != nil {
			return strSizeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "SETRANGE":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		off, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		if off < 0 {
			return AppendError(reply, "ERR offset is out of range")
		}
		n, err := s.s.SetRange(ctx, args[1], off, args[3])
		if err != nil {
			return strSizeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "INCR", "DECR", "INCRBY", "DECRBY":
		var delta int64
		switch cmd {
		case "INCR", "DECR":
			if len(args) != 2 {
				return arityErr(reply, cmd)
			}
			delta = 1
			if cmd == "DECR" {
				delta = -1
			}
		default:
			if len(args) != 3 {
				return arityErr(reply, cmd)
			}
			// string2ll strictness: Redis rejects "+1" and "01" as
			// increments, so the delta takes the canonical parser the
			// values themselves go through.
			n, ok := parseCanonicalInt(args[2])
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if cmd == "DECRBY" {
				// -MinInt64 has no int64 form; Redis words this one
				// differently from the value-overflow error.
				if n == math.MinInt64 {
					return AppendError(reply, "ERR decrement would overflow")
				}
				n = -n
			}
			delta = n
		}
		n, err := s.s.IncrBy(ctx, args[1], delta)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "MGET":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		mark := len(reply)
		reply = AppendArray(reply, len(args)-1)
		err := s.s.MGet(ctx, args[1:], func(v []byte, ok bool) {
			if ok {
				reply = AppendBulk(reply, v)
			} else {
				reply = AppendNullBulk(reply)
			}
		})
		if err != nil {
			// A partial array is already in the buffer; truncate back
			// to the mark so the error is the whole reply.
			return storeErr(reply[:mark], err)
		}
		return reply
	case "MSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return arityErr(reply, cmd)
		}
		s.splitPairs(args[1:])
		if err := s.s.MSet(ctx, s.mkeys, s.mvals); err != nil {
			return storeErr(reply, err)
		}
		// MSET is SET without options key by key, so each key's TTL
		// is discarded the way setCmd discards it.
		for _, k := range s.mkeys {
			if _, err := s.s.ExpireAt(ctx, k, 0); err != nil {
				return storeErr(reply, err)
			}
		}
		return AppendSimple(reply, "OK")
	case "MSETNX":
		if len(args) < 3 || len(args)%2 == 0 {
			return arityErr(reply, cmd)
		}
		s.splitPairs(args[1:])
		any, err := s.s.ExistsAny(ctx, s.mkeys)
		if err != nil {
			return storeErr(reply, err)
		}
		if any {
			return AppendInt(reply, 0)
		}
		if err := s.s.MSet(ctx, s.mkeys, s.mvals); err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, 1)
	case "INCRBYFLOAT":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		f, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil || math.IsNaN(f) {
			return AppendError(reply, "ERR value is not a valid float")
		}
		v, err := s.s.IncrByFloat(ctx, args[1], f)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendBulk(reply, v)
	case "SETBIT":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		off, ok := parseBitOffset(args[2])
		if !ok {
			return AppendError(reply, "ERR bit offset is not an integer or out of range")
		}
		b, ok := parseCanonicalInt(args[3])
		if !ok || (b != 0 && b != 1) {
			return AppendError(reply, "ERR bit is not an integer or out of range")
		}
		old, err := s.s.SetBit(ctx, args[1], off, int(b))
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, int64(old))
	case "GETBIT":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		off, ok := parseBitOffset(args[2])
		if !ok {
			return AppendError(reply, "ERR bit offset is not an integer or out of range")
		}
		bit, err := s.s.GetBit(ctx, args[1], off)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, int64(bit))
	case "BITFIELD", "BITFIELD_RO":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		ops, errText := s.parseBitfieldOps(args[2:], cmd == "BITFIELD_RO")
		if errText != "" {
			return AppendError(reply, errText)
		}
		res, nulls, err := s.s.Bitfield(ctx, args[1], ops)
		if err != nil {
			return storeErr(reply, err)
		}
		reply = AppendArray(reply, len(res))
		for i := range res {
			if nulls[i] {
				reply = AppendNullBulk(reply)
			} else {
				reply = AppendInt(reply, res[i])
			}
		}
		return reply
	case "BITCOUNT":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		var br bitRange
		switch len(args) {
		case 2:
		case 4, 5:
			start, ok1 := parseCanonicalInt(args[2])
			end, ok2 := parseCanonicalInt(args[3])
			if !ok1 || !ok2 {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			br = bitRange{start: start, end: end, ranged: true, endGiven: true}
			if len(args) == 5 {
				switch strings.ToUpper(string(args[4])) {
				case "BYTE":
				case "BIT":
					br.bitUnit = true
				default:
					return AppendError(reply, "ERR syntax error")
				}
			}
		default:
			return AppendError(reply, "ERR syntax error")
		}
		n, err := s.s.BitCount(ctx, args[1], br)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "BITPOS":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		if len(args) > 6 {
			return AppendError(reply, "ERR syntax error")
		}
		bit, ok := parseCanonicalInt(args[2])
		if !ok {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		if bit != 0 && bit != 1 {
			return AppendError(reply, "ERR The bit argument must be 1 or 0.")
		}
		br := bitRange{end: -1}
		if len(args) >= 4 {
			start, ok := parseCanonicalInt(args[3])
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			br.start, br.ranged = start, true
		}
		if len(args) >= 5 {
			end, ok := parseCanonicalInt(args[4])
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			br.end, br.endGiven = end, true
		}
		if len(args) == 6 {
			switch strings.ToUpper(string(args[5])) {
			case "BYTE":
			case "BIT":
				br.bitUnit = true
			default:
				return AppendError(reply, "ERR syntax error")
			}
		}
		pos, err := s.s.BitPos(ctx, args[1], int(bit), br)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, pos)
	case "BITOP":
		if len(args) < 4 {
			return arityErr(reply, cmd)
		}
		var op int
		switch strings.ToUpper(string(args[1])) {
		case "AND":
			op = bitopAnd
		case "OR":
			op = bitopOr
		case "XOR":
			op = bitopXor
		case "NOT":
			op = bitopNot
			if len(args) != 4 {
				return AppendError(reply, "ERR BITOP NOT must be called with a single source key.")
			}
		default:
			return syntaxErr(reply)
		}
		n, err := s.s.BitOp(ctx, op, args[2], args[3:])
		if err != nil {
			return storeErr(reply, err)
		}
		// BITOP is a store into dest, so like SET and MSET the
		// destination's old TTL is discarded.
		if _, err := s.s.ExpireAt(ctx, args[2], 0); err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "PFADD":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.s.PfAdd(ctx, args[1], args[2:])
		if err != nil {
			return hllErr(reply, err)
		}
		return AppendInt(reply, n)
	case "PFCOUNT":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.s.PfCount(ctx, args[1:])
		if err != nil {
			return hllErr(reply, err)
		}
		return AppendInt(reply, n)
	case "PFMERGE":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		if err := s.s.PfMerge(ctx, args[1], args[2:]); err != nil {
			return hllErr(reply, err)
		}
		return AppendSimple(reply, "OK")
	case "PFSELFTEST":
		if len(args) != 1 {
			return arityErr(reply, cmd)
		}
		if err := hllSelfTest(); err != nil {
			return AppendError(reply, err.Error())
		}
		return AppendSimple(reply, "OK")
	case "PFDEBUG":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		return s.pfdebugCmd(ctx, reply, args)
	case "LCS":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		return s.lcsCmd(ctx, reply, args)
	case "HSET", "HMSET":
		if len(args) < 4 || len(args)%2 != 0 {
			return arityErr(reply, cmd)
		}
		created := int64(0)
		for i := 2; i < len(args); i += 2 {
			c, err := s.h.HSet(ctx, args[1], args[i], args[i+1])
			if err != nil {
				return storeErr(reply, err)
			}
			if c {
				created++
			}
		}
		if cmd == "HMSET" {
			return AppendSimple(reply, "OK")
		}
		return AppendInt(reply, created)
	case "HSETNX":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		set, err := s.h.HSetNX(ctx, args[1], args[2], args[3])
		if err != nil {
			return storeErr(reply, err)
		}
		if set {
			return AppendInt(reply, 1)
		}
		return AppendInt(reply, 0)
	case "HGET":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		v, ok, err := s.h.HGet(ctx, args[1], args[2])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, v)
	case "HMGET":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		mark := len(reply)
		reply = AppendArray(reply, len(args)-2)
		err := s.h.HMGet(ctx, args[1], args[2:], func(v []byte, ok bool) {
			if ok {
				reply = AppendBulk(reply, v)
			} else {
				reply = AppendNullBulk(reply)
			}
		})
		if err != nil {
			// A partial array is already in the buffer; truncate back
			// to the mark so the error is the whole reply.
			return storeErr(reply[:mark], err)
		}
		return reply
	case "HDEL":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		n := int64(0)
		for _, f := range args[2:] {
			removed, err := s.h.HDel(ctx, args[1], f)
			if err != nil {
				return storeErr(reply, err)
			}
			if removed {
				n++
			}
		}
		return AppendInt(reply, n)
	case "HEXISTS", "HSTRLEN":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		v, ok, err := s.h.HGet(ctx, args[1], args[2])
		if err != nil {
			return storeErr(reply, err)
		}
		switch {
		case cmd == "HSTRLEN":
			return AppendInt(reply, int64(len(v)))
		case ok:
			return AppendInt(reply, 1)
		}
		return AppendInt(reply, 0)
	case "HLEN":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.h.HLen(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "HINCRBY":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		delta, ok := parseCanonicalInt(args[3])
		if !ok {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		n, err := s.h.HIncrBy(ctx, args[1], args[2], delta)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "HINCRBYFLOAT":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		f, err := strconv.ParseFloat(string(args[3]), 64)
		if err != nil || math.IsNaN(f) {
			return AppendError(reply, "ERR value is not a valid float")
		}
		v, err := s.h.HIncrByFloat(ctx, args[1], args[2], f)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendBulk(reply, v)
	case "HGETEX":
		return s.hgetexCmd(ctx, reply, args, now)
	case "HGETDEL":
		return s.hgetdelCmd(ctx, reply, args)
	case "HGETALL", "HKEYS", "HVALS":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		mark := len(reply)
		err := s.h.HIterate(ctx, args[1], func(n int) {
			if cmd == "HGETALL" {
				n *= 2
			}
			reply = AppendArray(reply, n)
		}, func(f, v []byte) {
			if cmd != "HVALS" {
				reply = AppendBulk(reply, f)
			}
			if cmd != "HKEYS" {
				reply = AppendBulk(reply, v)
			}
		})
		if err != nil {
			// A partial array is already in the buffer; truncate back
			// to the mark so the error is the whole reply.
			return storeErr(reply[:mark], err)
		}
		return reply
	case "HSCAN":
		return s.hscanCmd(ctx, reply, args)
	case "HRANDFIELD":
		return s.hrandCmd(ctx, reply, args)
	case "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT":
		return s.hexpireCmd(ctx, reply, args, now, cmd)
	case "HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME":
		return s.httlCmd(ctx, reply, args, now, cmd)
	case "SADD", "SREM":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		n := int64(0)
		for _, m := range args[2:] {
			var changed bool
			var err error
			if cmd == "SADD" {
				changed, err = s.se.SAdd(ctx, args[1], m)
			} else {
				changed, err = s.se.SRem(ctx, args[1], m)
			}
			if err != nil {
				return storeErr(reply, err)
			}
			if changed {
				n++
			}
		}
		return AppendInt(reply, n)
	case "SISMEMBER":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		ok, err := s.se.SIsMember(ctx, args[1], args[2])
		if err != nil {
			return storeErr(reply, err)
		}
		if ok {
			return AppendInt(reply, 1)
		}
		return AppendInt(reply, 0)
	case "SMISMEMBER":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		mark := len(reply)
		reply = AppendArray(reply, len(args)-2)
		err := s.se.SMIsMember(ctx, args[1], args[2:], func(ok bool) {
			if ok {
				reply = AppendInt(reply, 1)
			} else {
				reply = AppendInt(reply, 0)
			}
		})
		if err != nil {
			// A partial array is already in the buffer; truncate back
			// to the mark so the error is the whole reply.
			return storeErr(reply[:mark], err)
		}
		return reply
	case "SCARD":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.se.SCard(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "SMOVE":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		moved, err := s.se.SMove(ctx, args[1], args[2], args[3])
		if err != nil {
			return storeErr(reply, err)
		}
		if moved {
			return AppendInt(reply, 1)
		}
		return AppendInt(reply, 0)
	case "SMEMBERS":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		mark := len(reply)
		err := s.se.SMembers(ctx, args[1], func(n int) {
			reply = AppendArray(reply, n)
		}, func(m []byte) {
			reply = AppendBulk(reply, m)
		})
		if err != nil {
			// A partial array is already in the buffer; truncate back
			// to the mark so the error is the whole reply.
			return storeErr(reply[:mark], err)
		}
		return reply
	case "SSCAN":
		return s.sscanCmd(ctx, reply, args)
	case "SRANDMEMBER":
		return s.srandCmd(ctx, reply, args)
	case "SPOP":
		return s.spopCmd(ctx, reply, args)
	case "SINTER":
		return s.setAlgebraCmd(ctx, reply, args, cmd, s.se.SInter)
	case "SUNION":
		return s.setAlgebraCmd(ctx, reply, args, cmd, s.se.SUnion)
	case "SDIFF":
		return s.setAlgebraCmd(ctx, reply, args, cmd, s.se.SDiff)
	case "SINTERCARD":
		return s.sintercardCmd(ctx, reply, args)
	case "SINTERSTORE":
		return s.setStoreCmd(ctx, reply, args, cmd, s.se.SInterStore)
	case "SUNIONSTORE":
		return s.setStoreCmd(ctx, reply, args, cmd, s.se.SUnionStore)
	case "SDIFFSTORE":
		return s.setStoreCmd(ctx, reply, args, cmd, s.se.SDiffStore)
	case "ZADD":
		return s.zaddCmd(ctx, reply, args)
	case "ZINCRBY":
		if len(args) != 4 {
			return arityErr(reply, cmd)
		}
		incr, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil || math.IsNaN(incr) {
			return AppendError(reply, "ERR value is not a valid float")
		}
		v, err := s.z.ZIncrBy(ctx, args[1], incr, args[3])
		if err != nil {
			return storeErr(reply, err)
		}
		var sb [32]byte
		return AppendBulk(reply, appendScore(sb[:0], v))
	case "ZREM":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		n := int64(0)
		for _, m := range args[2:] {
			removed, err := s.z.ZRem(ctx, args[1], m)
			if err != nil {
				return storeErr(reply, err)
			}
			if removed {
				n++
			}
		}
		return AppendInt(reply, n)
	case "ZSCORE":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		v, ok, err := s.z.ZScore(ctx, args[1], args[2])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		var sb [32]byte
		return AppendBulk(reply, appendScore(sb[:0], v))
	case "ZMSCORE":
		if len(args) < 3 {
			return arityErr(reply, cmd)
		}
		mark := len(reply)
		reply = AppendArray(reply, len(args)-2)
		err := s.z.ZMScore(ctx, args[1], args[2:], func(score float64, ok bool) {
			if ok {
				var sb [32]byte
				reply = AppendBulk(reply, appendScore(sb[:0], score))
			} else {
				reply = AppendNullBulk(reply)
			}
		})
		if err != nil {
			// A partial array is already in the buffer; truncate back
			// to the mark so the error is the whole reply.
			return storeErr(reply[:mark], err)
		}
		return reply
	case "ZCARD":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.z.ZCard(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "ZRANK":
		return s.zrankCmd(ctx, reply, args, cmd, s.z.ZRank)
	case "ZREVRANK":
		return s.zrankCmd(ctx, reply, args, cmd, s.z.ZRevRank)
	case "ZRANGE":
		return s.zrangeCmd(ctx, reply, args)
	case "ZREVRANGE":
		return s.zrevrangeCmd(ctx, reply, args)
	case "ZRANGEBYSCORE":
		return s.zrangebyscoreCmd(ctx, reply, args, cmd, false)
	case "ZREVRANGEBYSCORE":
		return s.zrangebyscoreCmd(ctx, reply, args, cmd, true)
	case "ZRANGEBYLEX":
		return s.zrangebylexCmd(ctx, reply, args, cmd, false)
	case "ZREVRANGEBYLEX":
		return s.zrangebylexCmd(ctx, reply, args, cmd, true)
	case "ZCOUNT":
		return s.zcountCmd(ctx, reply, args)
	case "ZLEXCOUNT":
		return s.zlexcountCmd(ctx, reply, args)
	case "ZRANGESTORE":
		return s.zrangestoreCmd(ctx, reply, args)
	case "ZPOPMIN":
		return s.zpopCmd(ctx, reply, args, cmd, false)
	case "ZPOPMAX":
		return s.zpopCmd(ctx, reply, args, cmd, true)
	case "ZMPOP":
		return s.zmpopCmd(ctx, reply, args)
	case "BZPOPMIN":
		return s.bzpopCmd(ctx, reply, args, cmd, false)
	case "BZPOPMAX":
		return s.bzpopCmd(ctx, reply, args, cmd, true)
	case "BZMPOP":
		return s.bzmpopCmd(ctx, reply, args)
	case "ZRANDMEMBER":
		return s.zrandCmd(ctx, reply, args)
	case "ZREMRANGEBYRANK":
		return s.zremrangeCmd(ctx, reply, args, cmd, zrangeByIndex)
	case "ZREMRANGEBYSCORE":
		return s.zremrangeCmd(ctx, reply, args, cmd, zrangeByScore)
	case "ZREMRANGEBYLEX":
		return s.zremrangeCmd(ctx, reply, args, cmd, zrangeByLex)
	case "ZUNION":
		return s.zsetopCmd(ctx, reply, args, cmd, false, false)
	case "ZINTER":
		return s.zsetopCmd(ctx, reply, args, cmd, true, false)
	case "ZUNIONSTORE":
		return s.zsetopCmd(ctx, reply, args, cmd, false, true)
	case "ZINTERSTORE":
		return s.zsetopCmd(ctx, reply, args, cmd, true, true)
	case "ZDIFF":
		return s.zdiffCmd(ctx, reply, args, cmd, false)
	case "ZDIFFSTORE":
		return s.zdiffCmd(ctx, reply, args, cmd, true)
	case "ZINTERCARD":
		return s.zintercardCmd(ctx, reply, args)
	case "ZSCAN":
		return s.zscanCmd(ctx, reply, args)
	case "GEOADD":
		return s.geoaddCmd(ctx, reply, args)
	case "GEOPOS":
		return s.geoposCmd(ctx, reply, args)
	case "GEODIST":
		return s.geodistCmd(ctx, reply, args)
	case "GEOHASH":
		return s.geohashCmd(ctx, reply, args)
	case "GEOSEARCH":
		return s.geosearchCmd(ctx, reply, args, false)
	case "GEOSEARCHSTORE":
		return s.geosearchCmd(ctx, reply, args, true)
	case "GEORADIUS":
		return s.georadiusCmd(ctx, reply, args, cmd, false, false)
	case "GEORADIUS_RO":
		return s.georadiusCmd(ctx, reply, args, cmd, false, true)
	case "GEORADIUSBYMEMBER":
		return s.georadiusCmd(ctx, reply, args, cmd, true, false)
	case "GEORADIUSBYMEMBER_RO":
		return s.georadiusCmd(ctx, reply, args, cmd, true, true)
	case "HPERSIST":
		return s.hpersistCmd(ctx, reply, args)
	case "LPUSH":
		return s.lpushCmd(ctx, reply, args, cmd, true, false)
	case "RPUSH":
		return s.lpushCmd(ctx, reply, args, cmd, false, false)
	case "LPUSHX":
		return s.lpushCmd(ctx, reply, args, cmd, true, true)
	case "RPUSHX":
		return s.lpushCmd(ctx, reply, args, cmd, false, true)
	case "LPOP":
		return s.lpopCmd(ctx, reply, args, cmd, true)
	case "RPOP":
		return s.lpopCmd(ctx, reply, args, cmd, false)
	case "LMPOP":
		return s.lmpopCmd(ctx, reply, args)
	case "BLPOP":
		return s.blpopCmd(ctx, reply, args, cmd, true)
	case "BRPOP":
		return s.blpopCmd(ctx, reply, args, cmd, false)
	case "BLMPOP":
		return s.blmpopCmd(ctx, reply, args)
	case "LMOVE":
		return s.lmoveCmd(ctx, reply, args)
	case "RPOPLPUSH":
		return s.rpoplpushCmd(ctx, reply, args)
	case "BLMOVE":
		return s.blmoveCmd(ctx, reply, args)
	case "BRPOPLPUSH":
		return s.brpoplpushCmd(ctx, reply, args)
	case "LLEN":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.l.Len(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "LINDEX":
		return s.lindexCmd(ctx, reply, args)
	case "LSET":
		return s.lsetCmd(ctx, reply, args)
	case "LRANGE":
		return s.lrangeCmd(ctx, reply, args)
	case "LTRIM":
		return s.ltrimCmd(ctx, reply, args)
	case "LINSERT":
		return s.linsertCmd(ctx, reply, args)
	case "LREM":
		return s.lremCmd(ctx, reply, args)
	case "LPOS":
		return s.lposCmd(ctx, reply, args)
	case "XADD":
		return s.xaddCmd(ctx, reply, args, now)
	case "XLEN":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		n, err := s.x.Len(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	case "XTRIM":
		return s.xtrimCmd(ctx, reply, args)
	case "XDEL":
		return s.xdelCmd(ctx, reply, args)
	case "XSETID":
		return s.xsetidCmd(ctx, reply, args)
	case "XINFO":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		return s.xinfoCmd(ctx, reply, args, now)
	case "XGROUP":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		return s.xgroupCmd(ctx, reply, args, now)
	case "XRANGE":
		return s.xrangeCmd(ctx, reply, args, false)
	case "XREVRANGE":
		return s.xrangeCmd(ctx, reply, args, true)
	case "XREAD":
		return s.xreadCmd(ctx, reply, args)
	case "XREADGROUP":
		return s.xreadgroupCmd(ctx, reply, args, now)
	case "XACK":
		return s.xackCmd(ctx, reply, args)
	case "XPENDING":
		return s.xpendingCmd(ctx, reply, args, now)
	case "XCLAIM":
		return s.xclaimCmd(ctx, reply, args, now)
	case "XAUTOCLAIM":
		return s.xautoclaimCmd(ctx, reply, args, now)
	case "TYPE":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		v, root, _, ok, err := s.t.LookupEntry(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendSimple(reply, "none")
		}
		if root {
			tag, _, err := sniffRoot(v)
			if err != nil {
				return storeErr(reply, err)
			}
			switch tag {
			case TagHash:
				return AppendSimple(reply, "hash")
			case TagSet:
				return AppendSimple(reply, "set")
			case TagZset:
				return AppendSimple(reply, "zset")
			case TagList:
				return AppendSimple(reply, "list")
			case TagStream:
				return AppendSimple(reply, "stream")
			}
		}
		return AppendSimple(reply, "string")
	case "OBJECT":
		if len(args) == 3 && strings.EqualFold(string(args[1]), "ENCODING") {
			v, root, _, ok, err := s.t.LookupEntry(ctx, args[2])
			if err != nil {
				return storeErr(reply, err)
			}
			if !ok {
				// Redis 8.8 replies null bulk here, not the "no
				// such key" error older versions used.
				return AppendNullBulk(reply)
			}
			if root {
				tag, _, err := sniffRoot(v)
				if err != nil {
					return storeErr(reply, err)
				}
				switch tag {
				case TagHash:
					enc, ok, err := s.h.Encoding(ctx, args[2])
					if err != nil {
						return storeErr(reply, err)
					}
					if !ok {
						return AppendNullBulk(reply)
					}
					return AppendBulk(reply, []byte(enc))
				case TagSet:
					enc, ok, err := s.se.Encoding(ctx, args[2])
					if err != nil {
						return storeErr(reply, err)
					}
					if !ok {
						return AppendNullBulk(reply)
					}
					return AppendBulk(reply, []byte(enc))
				case TagZset:
					enc, ok, err := s.z.Encoding(ctx, args[2])
					if err != nil {
						return storeErr(reply, err)
					}
					if !ok {
						return AppendNullBulk(reply)
					}
					return AppendBulk(reply, []byte(enc))
				case TagList:
					enc, ok, err := s.l.Encoding(ctx, args[2])
					if err != nil {
						return storeErr(reply, err)
					}
					if !ok {
						return AppendNullBulk(reply)
					}
					return AppendBulk(reply, []byte(enc))
				case TagStream:
					// Streams have one encoding at every size.
					return AppendBulk(reply, []byte("stream"))
				}
			}
			enc, ok, err := s.s.Encoding(ctx, args[2])
			if err != nil {
				return storeErr(reply, err)
			}
			if !ok {
				return AppendNullBulk(reply)
			}
			return AppendBulk(reply, []byte(enc))
		}
		return AppendError(reply, "ERR unknown subcommand or wrong number of arguments for 'OBJECT'")
	case "DEL":
		if len(args) < 2 {
			return arityErr(reply, cmd)
		}
		n := int64(0)
		for _, k := range args[1:] {
			dead, err := s.s.Del(ctx, k)
			if err != nil {
				return storeErr(reply, err)
			}
			if dead {
				n++
			}
		}
		return AppendInt(reply, n)
	case "EXPIRE":
		if len(args) != 3 {
			return arityErr(reply, cmd)
		}
		sec, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		exists, _, err := s.s.Entry(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !exists {
			return AppendInt(reply, 0)
		}
		if sec <= 0 {
			if _, err := s.s.Del(ctx, args[1]); err != nil {
				return storeErr(reply, err)
			}
			return AppendInt(reply, 1)
		}
		at, ok := expireFrom(now, sec, 1000)
		if !ok {
			return invalidExpire(reply, cmd)
		}
		if _, err := s.s.ExpireAt(ctx, args[1], at); err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, 1)
	case "TTL":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		exists, expMs, err := s.s.Entry(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		switch {
		case !exists:
			return AppendInt(reply, -2)
		case expMs == 0:
			return AppendInt(reply, -1)
		default:
			// Round up, so a key with 1ms left still reports 1.
			return AppendInt(reply, (expMs-now+999)/1000)
		}
	}
	return AppendError(reply, fmt.Sprintf("ERR unknown command '%s'", args[0]))
}

// appendScore formats a zset score the way Redis replies one,
// d2string's shape: integral values inside the int64 span print as
// integers, everything else as the shortest round-trip float, and the
// infinities as inf and -inf. The sortable codec folds -0 into +0, so
// a stored score never prints the negative zero.
func appendScore(dst []byte, f float64) []byte {
	switch {
	case math.IsInf(f, 1):
		return append(dst, "inf"...)
	case math.IsInf(f, -1):
		return append(dst, "-inf"...)
	case f == math.Trunc(f) && f >= -9.223372036854776e18 && f < 9.223372036854776e18:
		return strconv.AppendInt(dst, int64(f), 10)
	}
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

// zrankCmd is ZRANK and ZREVRANK behind their shared walk: an
// integer rank, or [rank, score] under WITHSCORE, with the nil shape
// following the reply shape (null bulk plain, null array WITHSCORE),
// Redis's zrankGenericCommand surface.
func (s *Server) zrankCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, rank func(context.Context, []byte, []byte) (int64, float64, bool, error)) []byte {
	if len(args) < 3 {
		return arityErr(reply, cmd)
	}
	withScore := len(args) == 4 && strings.EqualFold(string(args[3]), "WITHSCORE")
	if len(args) > 3 && !withScore {
		return AppendError(reply, "ERR syntax error")
	}
	r, score, ok, err := rank(ctx, args[1], args[2])
	if err != nil {
		return storeErr(reply, err)
	}
	if !ok {
		if withScore {
			return AppendNullArray(reply)
		}
		return AppendNullBulk(reply)
	}
	if !withScore {
		return AppendInt(reply, r)
	}
	reply = AppendArray(reply, 2)
	reply = AppendInt(reply, r)
	var sb [32]byte
	return AppendBulk(reply, appendScore(sb[:0], score))
}

// zaddCmd is ZADD with the full option surface: NX, XX, GT, LT, CH,
// INCR. CH swaps the reply from members created to members touched;
// INCR turns the one allowed pair into ZINCRBY with a nil reply when
// a condition flag vetoes the write.
func (s *Server) zaddCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "ZADD")
	}
	var f ZAddFlags
	ch := false
	i := 2
flags:
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			f.NX = true
		case "XX":
			f.XX = true
		case "GT":
			f.GT = true
		case "LT":
			f.LT = true
		case "CH":
			ch = true
		case "INCR":
			f.Incr = true
		default:
			break flags
		}
	}
	if f.NX && f.XX {
		return AppendError(reply, "ERR XX and NX options at the same time are not compatible")
	}
	if (f.GT && f.NX) || (f.LT && f.NX) || (f.GT && f.LT) {
		return AppendError(reply, "ERR GT, LT, and/or NX options at the same time are not compatible")
	}
	pairs := args[i:]
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return syntaxErr(reply)
	}
	if f.Incr && len(pairs) != 2 {
		return AppendError(reply, "ERR INCR option supports a single increment-element pair")
	}
	s.zscores = s.zscores[:0]
	for j := 0; j < len(pairs); j += 2 {
		v, err := strconv.ParseFloat(string(pairs[j]), 64)
		if err != nil || math.IsNaN(v) {
			return AppendError(reply, "ERR value is not a valid float")
		}
		s.zscores = append(s.zscores, v)
	}
	added, touched := int64(0), int64(0)
	for j := 0; j < len(pairs); j += 2 {
		a, c, out, outOK, err := s.z.ZAdd(ctx, args[1], pairs[j+1], s.zscores[j/2], f)
		if err != nil {
			return storeErr(reply, err)
		}
		if f.Incr {
			if !outOK {
				return AppendNullBulk(reply)
			}
			var sb [32]byte
			return AppendBulk(reply, appendScore(sb[:0], out))
		}
		if a {
			added++
			touched++
		} else if c {
			touched++
		}
	}
	if ch {
		return AppendInt(reply, touched)
	}
	return AppendInt(reply, added)
}

// setCmd is SET with the full option surface: NX, XX, GET, KEEPTTL,
// EX, PX, EXAT, PXAT. The TTL rule is Redis's: a plain SET discards
// any TTL, KEEPTTL keeps it, and the expiry options set one.
func (s *Server) setCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 3 {
		return arityErr(reply, "SET")
	}
	key, val := args[1], args[2]
	var nx, xx, get, keepttl, hasExp bool
	var expAt int64
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepttl = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExp || i+1 == len(args) {
				return syntaxErr(reply)
			}
			opt := strings.ToUpper(string(args[i]))
			i++
			n, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			var ok bool
			switch opt {
			case "EX":
				expAt, ok = expireFrom(now, n, 1000)
				ok = ok && n > 0
			case "PX":
				expAt, ok = expireFrom(now, n, 1)
				ok = ok && n > 0
			case "EXAT":
				expAt, ok = expireFrom(0, n, 1000)
			case "PXAT":
				expAt, ok = n, true
			}
			if !ok {
				return invalidExpire(reply, "SET")
			}
			hasExp = true
		default:
			return syntaxErr(reply)
		}
	}
	if (nx && xx) || (hasExp && keepttl) {
		return syntaxErr(reply)
	}

	oldOk := false
	if nx || xx || get {
		if get {
			v, ok, err := s.s.Get(ctx, key)
			if err != nil {
				return storeErr(reply, err)
			}
			oldOk = ok
			if ok {
				// The write below recycles the arena bytes v aliases.
				s.old = append(s.old[:0], v...)
			}
		} else {
			var err error
			oldOk, _, err = s.s.Entry(ctx, key)
			if err != nil {
				return storeErr(reply, err)
			}
		}
	}
	if (nx && oldOk) || (xx && !oldOk) {
		if get {
			if !oldOk {
				return AppendNullBulk(reply)
			}
			return AppendBulk(reply, s.old)
		}
		return AppendNullBulk(reply)
	}

	if err := s.s.Set(ctx, key, val); err != nil {
		return storeErr(reply, err)
	}
	if !keepttl {
		// expAt is 0 without an expiry option, and stamping 0 is the
		// discard; PutGen keeps a live key's expiry on purpose, so the
		// discard is this layer's job. An EXAT or PXAT already in the
		// past deletes outright: the observable state matches Redis,
		// and the delete retires a rope's plane instead of leaving
		// expiry-orphaned chunks.
		var err error
		if hasExp && expAt <= now {
			_, err = s.s.Del(ctx, key)
		} else {
			_, err = s.s.ExpireAt(ctx, key, expAt)
		}
		if err != nil {
			return storeErr(reply, err)
		}
	}
	if get {
		if !oldOk {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, s.old)
	}
	return AppendSimple(reply, "OK")
}

// getexCmd is GETEX: GET plus an optional expiry edit. A past EXAT or
// PXAT deletes the key after the read, like Redis; no option reads
// without touching the TTL.
func (s *Server) getexCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 2 {
		return arityErr(reply, "GETEX")
	}
	key := args[1]
	var persist, hasExp bool
	var expAt int64
	for i := 2; i < len(args); i++ {
		switch opt := strings.ToUpper(string(args[i])); opt {
		case "PERSIST":
			if persist || hasExp {
				return syntaxErr(reply)
			}
			persist = true
		case "EX", "PX", "EXAT", "PXAT":
			if persist || hasExp || i+1 == len(args) {
				return syntaxErr(reply)
			}
			i++
			n, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			var ok bool
			switch opt {
			case "EX":
				expAt, ok = expireFrom(now, n, 1000)
				ok = ok && n > 0
			case "PX":
				expAt, ok = expireFrom(now, n, 1)
				ok = ok && n > 0
			case "EXAT":
				expAt, ok = expireFrom(0, n, 1000)
			case "PXAT":
				expAt, ok = n, true
			}
			if !ok {
				return invalidExpire(reply, "GETEX")
			}
			hasExp = true
		default:
			return syntaxErr(reply)
		}
	}

	v, ok, err := s.s.Get(ctx, key)
	if err != nil {
		return storeErr(reply, err)
	}
	if !ok {
		return AppendNullBulk(reply)
	}
	s.old = append(s.old[:0], v...)
	switch {
	case persist:
		_, err = s.s.ExpireAt(ctx, key, 0)
	case hasExp && expAt <= now:
		_, err = s.s.Del(ctx, key)
	case hasExp:
		_, err = s.s.ExpireAt(ctx, key, expAt)
	}
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendBulk(reply, s.old)
}

// hgetexCmd is HGETEX key [EX n | PX n | EXAT n | PXAT n | PERSIST]
// FIELDS numfields field...: a read with an optional field-TTL edit,
// at most one option, mirroring getexCmd's parse. A past EXAT or PXAT
// deletes the field after the read, GETEX's key-level rule applied
// per field, which is HGETDEL's observable behavior exactly.
func (s *Server) hgetexCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 5 {
		return arityErr(reply, "HGETEX")
	}
	var persist, hasExp bool
	var expAt int64
	i := 2
loop:
	for i < len(args) {
		switch opt := strings.ToUpper(string(args[i])); opt {
		case "PERSIST":
			if persist || hasExp {
				return syntaxErr(reply)
			}
			persist = true
			i++
		case "EX", "PX", "EXAT", "PXAT":
			if persist || hasExp || i+1 == len(args) {
				return syntaxErr(reply)
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			var ok bool
			switch opt {
			case "EX":
				expAt, ok = expireFrom(now, n, 1000)
				ok = ok && n > 0
			case "PX":
				expAt, ok = expireFrom(now, n, 1)
				ok = ok && n > 0
			case "EXAT":
				expAt, ok = expireFrom(0, n, 1000)
			case "PXAT":
				expAt, ok = n, true
			}
			if !ok {
				return invalidExpire(reply, "HGETEX")
			}
			hasExp = true
			i += 2
		default:
			break loop
		}
	}
	fields, errText := fieldsBlock(args[i:])
	if errText != "" {
		return AppendError(reply, errText)
	}
	edit := persist || hasExp
	mark := len(reply)
	reply = AppendArray(reply, len(fields))
	for _, f := range fields {
		var v []byte
		var ok bool
		var err error
		if hasExp && expAt <= now {
			v, ok, err = s.h.HGetDel(ctx, args[1], f)
		} else {
			v, ok, err = s.h.HGetEx(ctx, args[1], f, edit, expAt)
		}
		if err != nil {
			return storeErr(reply[:mark], err)
		}
		if ok {
			reply = AppendBulk(reply, v)
		} else {
			reply = AppendNullBulk(reply)
		}
	}
	return reply
}

// hgetdelCmd is HGETDEL key FIELDS numfields field...: read and
// remove, one reply entry per field.
func (s *Server) hgetdelCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 5 {
		return arityErr(reply, "HGETDEL")
	}
	fields, errText := fieldsBlock(args[2:])
	if errText != "" {
		return AppendError(reply, errText)
	}
	mark := len(reply)
	reply = AppendArray(reply, len(fields))
	for _, f := range fields {
		v, ok, err := s.h.HGetDel(ctx, args[1], f)
		if err != nil {
			return storeErr(reply[:mark], err)
		}
		if ok {
			reply = AppendBulk(reply, v)
		} else {
			reply = AppendNullBulk(reply)
		}
	}
	return reply
}

// hscanCmd is HSCAN key cursor [MATCH pattern] [COUNT count]
// [NOVALUES], Redis's option grammar: options repeat with last-wins,
// COUNT below one is a syntax error, anything unknown is too. The
// step's elements stage in scanBuf because MATCH decides the element
// count only after the walk.
func (s *Server) hscanCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "HSCAN")
	}
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return AppendError(reply, "ERR invalid cursor")
	}
	count := int64(10)
	var match []byte
	hasMatch, noValues := false, false
	for i := 3; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if n < 1 {
				return syntaxErr(reply)
			}
			count = n
			i++
		case strings.EqualFold(string(args[i]), "MATCH") && i+1 < len(args):
			match, hasMatch = args[i+1], true
			i++
		case strings.EqualFold(string(args[i]), "NOVALUES"):
			noValues = true
		default:
			return syntaxErr(reply)
		}
	}
	s.scanBuf = s.scanBuf[:0]
	elems := 0
	next, err := s.h.HScan(ctx, args[1], cursor, count, func(f, v []byte) {
		if hasMatch && !globMatch(match, f) {
			return
		}
		s.scanBuf = AppendBulk(s.scanBuf, f)
		elems++
		if !noValues {
			s.scanBuf = AppendBulk(s.scanBuf, v)
			elems++
		}
	})
	if err != nil {
		return storeErr(reply, err)
	}
	var cbuf [20]byte
	reply = AppendArray(reply, 2)
	reply = AppendBulk(reply, strconv.AppendUint(cbuf[:0], next, 10))
	reply = AppendArray(reply, elems)
	return append(reply, s.scanBuf...)
}

// sscanCmd is SSCAN key cursor [MATCH pattern] [COUNT count], HSCAN's
// grammar without NOVALUES: options repeat with last-wins, COUNT below
// one is a syntax error, anything unknown is too, except that Redis
// parses NOVALUES here and rejects it with its own text (the scan
// grammar is shared, the option is gated after parsing). The step's
// members stage in scanBuf because MATCH decides the element count
// only after the walk.
func (s *Server) sscanCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "SSCAN")
	}
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return AppendError(reply, "ERR invalid cursor")
	}
	count := int64(10)
	var match []byte
	hasMatch := false
	for i := 3; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if n < 1 {
				return syntaxErr(reply)
			}
			count = n
			i++
		case strings.EqualFold(string(args[i]), "MATCH") && i+1 < len(args):
			match, hasMatch = args[i+1], true
			i++
		case strings.EqualFold(string(args[i]), "NOVALUES"):
			return AppendError(reply, "ERR NOVALUES option can only be used in HSCAN")
		default:
			return syntaxErr(reply)
		}
	}
	s.scanBuf = s.scanBuf[:0]
	elems := 0
	next, err := s.se.SScan(ctx, args[1], cursor, count, func(m []byte) {
		if hasMatch && !globMatch(match, m) {
			return
		}
		s.scanBuf = AppendBulk(s.scanBuf, m)
		elems++
	})
	if err != nil {
		return storeErr(reply, err)
	}
	var cbuf [20]byte
	reply = AppendArray(reply, 2)
	reply = AppendBulk(reply, strconv.AppendUint(cbuf[:0], next, 10))
	reply = AppendArray(reply, elems)
	return append(reply, s.scanBuf...)
}

// zscanCmd is ZSCAN key cursor [MATCH pattern] [COUNT count], the
// shared scan grammar over the zset member family: options repeat
// with last-wins, COUNT below one is a syntax error, anything unknown
// is too, and NOVALUES parses here only to be rejected with HSCAN's
// ownership text (probed on Redis 8.8.0, SSCAN's door verbatim).
// Every emitted member is followed by its score through the shared
// formatter; MATCH filters members only.
func (s *Server) zscanCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "ZSCAN")
	}
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return AppendError(reply, "ERR invalid cursor")
	}
	count := int64(10)
	var match []byte
	hasMatch := false
	for i := 3; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if n < 1 {
				return syntaxErr(reply)
			}
			count = n
			i++
		case strings.EqualFold(string(args[i]), "MATCH") && i+1 < len(args):
			match, hasMatch = args[i+1], true
			i++
		case strings.EqualFold(string(args[i]), "NOVALUES"):
			return AppendError(reply, "ERR NOVALUES option can only be used in HSCAN")
		default:
			return syntaxErr(reply)
		}
	}
	s.scanBuf = s.scanBuf[:0]
	elems := 0
	var sb [32]byte
	next, err := s.z.ZScan(ctx, args[1], cursor, count, func(m []byte, sc float64) {
		if hasMatch && !globMatch(match, m) {
			return
		}
		s.scanBuf = AppendBulk(s.scanBuf, m)
		s.scanBuf = AppendBulk(s.scanBuf, appendScore(sb[:0], sc))
		elems += 2
	})
	if err != nil {
		return storeErr(reply, err)
	}
	var cbuf [20]byte
	reply = AppendArray(reply, 2)
	reply = AppendBulk(reply, strconv.AppendUint(cbuf[:0], next, 10))
	reply = AppendArray(reply, elems)
	return append(reply, s.scanBuf...)
}

// srandCmd is SRANDMEMBER key [count], Redis's grammar: no count is
// one draw with a nil bulk on a missing key, a negative count draws
// with replacement, and anything after the count is a syntax error
// checked before the count parses.
func (s *Server) srandCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return arityErr(reply, "SRANDMEMBER")
	}
	if len(args) == 2 {
		m, ok, err := s.se.SRandMember(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, m)
	}
	if len(args) > 3 {
		return syntaxErr(reply)
	}
	l, ok := parseCanonicalInt(args[2])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if l == math.MinInt64 {
		return AppendError(reply, "ERR value is out of range")
	}
	count, withReplacement := l, false
	if l < 0 {
		count, withReplacement = -l, true
	}
	mark := len(reply)
	err := s.se.SRandMemberCount(ctx, args[1], count, withReplacement, func(n int64) {
		reply = AppendArray(reply, int(n))
	}, func(m []byte) {
		reply = AppendBulk(reply, m)
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}

// spopCmd is SPOP key [count]: one popped member as a bulk (nil on a
// missing key) without the count, an array with it. The count parse is
// Redis's exact door: a non-integer and a negative both answer
// out-of-range-must-be-positive, and a count of zero is an empty array
// with nothing removed.
func (s *Server) spopCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return arityErr(reply, "SPOP")
	}
	if len(args) == 2 {
		m, ok, err := s.se.SPop(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, m)
	}
	if len(args) > 3 {
		return syntaxErr(reply)
	}
	l, ok := parseCanonicalInt(args[2])
	if !ok || l < 0 {
		return AppendError(reply, "ERR value is out of range, must be positive")
	}
	mark := len(reply)
	err := s.se.SPopCount(ctx, args[1], l, func(n int64) {
		reply = AppendArray(reply, int(n))
	}, func(m []byte) {
		reply = AppendBulk(reply, m)
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}

// setAlgebraCmd runs one of the algebra streamers (SINTER, SUNION,
// SDIFF share the grammar: the command and at least one key) over
// args[1:]. The members stage in scanBuf because the element count is
// known only after the walk.
func (s *Server) setAlgebraCmd(ctx context.Context, reply []byte, args [][]byte, name string, walk func(ctx context.Context, keys [][]byte, emit func(member []byte)) error) []byte {
	if len(args) < 2 {
		return arityErr(reply, name)
	}
	s.scanBuf = s.scanBuf[:0]
	elems := 0
	if err := walk(ctx, args[1:], func(m []byte) {
		s.scanBuf = AppendBulk(s.scanBuf, m)
		elems++
	}); err != nil {
		return storeErr(reply, err)
	}
	reply = AppendArray(reply, elems)
	return append(reply, s.scanBuf...)
}

// setStoreCmd runs one of the algebra STORE variants (SINTERSTORE,
// SUNIONSTORE, SDIFFSTORE share the grammar: the command, the
// destination, and at least one source key) and answers the stored
// cardinality.
func (s *Server) setStoreCmd(ctx context.Context, reply []byte, args [][]byte, name string, store func(ctx context.Context, dest []byte, keys [][]byte) (int64, error)) []byte {
	if len(args) < 3 {
		return arityErr(reply, name)
	}
	n, err := store(ctx, args[1], args[2:])
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}

// sintercardCmd is SINTERCARD numkeys key [key ...] [LIMIT limit],
// Redis's exact doors in Redis's order: a numkeys that does not parse
// or is below one answers greater-than-0, a numkeys past the argument
// count answers the number-of-args text (counted against all
// remaining arguments, LIMIT tokens included, Redis's quirk), the
// tail must be empty or exactly LIMIT n with n >= 0, and LIMIT 0
// means unlimited.
func (s *Server) sintercardCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "SINTERCARD")
	}
	nk, ok := parseCanonicalInt(args[1])
	if !ok || nk < 1 {
		return AppendError(reply, "ERR numkeys should be greater than 0")
	}
	if nk > int64(len(args)-2) {
		return AppendError(reply, "ERR Number of keys can't be greater than number of args")
	}
	limit := int64(0)
	switch {
	case int64(len(args)) == nk+2:
	case int64(len(args)) == nk+4 && strings.EqualFold(string(args[nk+2]), "LIMIT"):
		l, ok := parseCanonicalInt(args[nk+3])
		if !ok || l < 0 {
			return AppendError(reply, "ERR LIMIT can't be negative")
		}
		limit = l
	default:
		return syntaxErr(reply)
	}
	card, err := s.se.SInterCard(ctx, args[2:2+nk], limit)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, card)
}

// hrandCmd is HRANDFIELD key [count [WITHVALUES]], Redis's exact
// grammar: the count parses first with a -LONG_MAX..LONG_MAX range
// check, anything after it other than a lone WITHVALUES is a syntax
// error, and WITHVALUES halves the legal range so the doubled reply
// length cannot overflow. A negative count draws with replacement.
func (s *Server) hrandCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return arityErr(reply, "HRANDFIELD")
	}
	if len(args) == 2 {
		f, _, ok, err := s.h.HRandField(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, f)
	}
	l, ok := parseCanonicalInt(args[2])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if l == math.MinInt64 {
		return AppendError(reply, "ERR value is out of range")
	}
	if len(args) > 4 || (len(args) == 4 && !strings.EqualFold(string(args[3]), "WITHVALUES")) {
		return syntaxErr(reply)
	}
	withValues := len(args) == 4
	if withValues && (l < -(math.MaxInt64/2) || l > math.MaxInt64/2) {
		return AppendError(reply, "ERR value is out of range")
	}
	count, withReplacement := l, false
	if l < 0 {
		count, withReplacement = -l, true
	}
	mark := len(reply)
	err := s.h.HRandFieldCount(ctx, args[1], count, withReplacement, func(n int64) {
		if withValues {
			n *= 2
		}
		reply = AppendArray(reply, int(n))
	}, func(f, v []byte) {
		reply = AppendBulk(reply, f)
		if withValues {
			reply = AppendBulk(reply, v)
		}
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}

// hfeMaxAbsTimeMs bounds every absolute field expiry, Redis's
// HFE_MAX_ABS_TIME_MSEC: the listpackEx TTL field steals two bits, so
// the ceiling is (2^48 - 1) >> 2. Ours has no such packing, but the
// bound is wire behavior the compat section diffs.
const hfeMaxAbsTimeMs = int64(1)<<46 - 1

// hexpireCmd is HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT key time
// [NX|XX|GT|LT] FIELDS numfields field..., Redis 8's exact grammar
// and check order: type first, then the expire parse, then the
// optional condition, then the FIELDS block, and a missing key is a
// -2 array only after all of that.
func (s *Server) hexpireCmd(ctx context.Context, reply []byte, args [][]byte, now int64, cmd string) []byte {
	if len(args) < 6 {
		return arityErr(reply, cmd)
	}
	if _, err := s.h.HLen(ctx, args[1]); err != nil {
		return storeErr(reply, err)
	}
	unit, base := int64(1000), now
	switch cmd {
	case "HPEXPIRE":
		unit = 1
	case "HEXPIREAT":
		base = 0
	case "HPEXPIREAT":
		unit, base = 1, 0
	}
	atMs, errReply := hfeExpireAt(reply, args[2], cmd, unit, base)
	if errReply != nil {
		return errReply
	}
	cond, fieldsAt := HExpireNone, 3
	switch strings.ToUpper(string(args[3])) {
	case "NX":
		cond, fieldsAt = HExpireNX, 4
	case "XX":
		cond, fieldsAt = HExpireXX, 4
	case "GT":
		cond, fieldsAt = HExpireGT, 4
	case "LT":
		cond, fieldsAt = HExpireLT, 4
	}
	if !strings.EqualFold(string(args[fieldsAt]), "FIELDS") {
		return AppendError(reply, "ERR unknown argument: "+string(args[fieldsAt]))
	}
	fields, errText := hfeFields(args[fieldsAt:],
		"ERR Parameter `numFields` should be greater than 0",
		"ERR wrong number of arguments")
	if errText != "" {
		return AppendError(reply, errText)
	}
	res, err := s.h.HExpire(ctx, args[1], atMs, cond, fields, s.ttlBuf[:0])
	if err != nil {
		return storeErr(reply, err)
	}
	s.ttlBuf = res
	reply = AppendArray(reply, len(res))
	for _, code := range res {
		reply = AppendInt(reply, code)
	}
	return reply
}

// httlCmd is HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME key FIELDS numfields
// field...: the engine answers absolute expire milliseconds and this
// layer owns Redis's four conversions, remaining seconds rounding up.
func (s *Server) httlCmd(ctx context.Context, reply []byte, args [][]byte, now int64, cmd string) []byte {
	if len(args) < 5 {
		return arityErr(reply, cmd)
	}
	if _, err := s.h.HLen(ctx, args[1]); err != nil {
		return storeErr(reply, err)
	}
	fields, errText := hfeFields(args[2:],
		"ERR Number of fields must be a positive integer",
		"ERR The `numfields` parameter must match the number of arguments")
	if errText != "" {
		return AppendError(reply, errText)
	}
	res, err := s.h.HTtl(ctx, args[1], fields, s.ttlBuf[:0])
	if err != nil {
		return storeErr(reply, err)
	}
	s.ttlBuf = res
	reply = AppendArray(reply, len(res))
	for _, e := range res {
		if e > 0 {
			switch cmd {
			case "HTTL":
				e = (e + 999 - now) / 1000
			case "HPTTL":
				e -= now
			case "HEXPIRETIME":
				e = (e + 999) / 1000
			}
		}
		reply = AppendInt(reply, e)
	}
	return reply
}

// hpersistCmd is HPERSIST key FIELDS numfields field...
func (s *Server) hpersistCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 5 {
		return arityErr(reply, "HPERSIST")
	}
	if _, err := s.h.HLen(ctx, args[1]); err != nil {
		return storeErr(reply, err)
	}
	fields, errText := hfeFields(args[2:],
		"ERR Number of fields must be a positive integer",
		"ERR The `numfields` parameter must match the number of arguments")
	if errText != "" {
		return AppendError(reply, errText)
	}
	res, err := s.h.HPersist(ctx, args[1], fields, s.ttlBuf[:0])
	if err != nil {
		return storeErr(reply, err)
	}
	s.ttlBuf = res
	reply = AppendArray(reply, len(res))
	for _, code := range res {
		reply = AppendInt(reply, code)
	}
	return reply
}

// hfeExpireAt is Redis's parseExpireTime: the value must be a
// non-negative integer, and neither the seconds-to-ms conversion nor
// the base add may cross hfeMaxAbsTimeMs. A non-nil errReply is the
// whole reply.
func hfeExpireAt(reply []byte, arg []byte, cmd string, unit, base int64) (atMs int64, errReply []byte) {
	val, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return 0, AppendError(reply, "ERR value is not an integer or out of range")
	}
	if val < 0 {
		return 0, AppendError(reply, "ERR invalid expire time, must be >= 0")
	}
	if unit == 1000 {
		if val > hfeMaxAbsTimeMs/1000 {
			return 0, invalidExpire(reply, cmd)
		}
		val *= 1000
	}
	if val > hfeMaxAbsTimeMs-base {
		return 0, invalidExpire(reply, cmd)
	}
	return base + val, nil
}

// hfeFields parses the FIELDS numfields field... run that ends the
// HEXPIRE family's commands. Same shape as fieldsBlock but with this
// family's own error texts, and 8.8 grew the set and read families
// apart (pinned live in the compat fixtures): the set family answers
// "unknown argument" for a misplaced FIELDS (its caller checks before
// calling here) and a plain "wrong number of arguments" on a
// numfields mismatch, while the read family kept the 8.0 texts.
func hfeFields(args [][]byte, numErrText, mismatchText string) ([][]byte, string) {
	if !strings.EqualFold(string(args[0]), "FIELDS") {
		return nil, "ERR Mandatory argument FIELDS is missing or not at the right position"
	}
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || n < 1 {
		return nil, numErrText
	}
	if n != int64(len(args)-2) {
		return nil, mismatchText
	}
	return args[2:], ""
}

// fieldsBlock parses the FIELDS numfields field... run that ends
// HGETEX and HGETDEL, with Redis's exact error texts. The non-empty
// return string is the whole error reply.
func fieldsBlock(args [][]byte) ([][]byte, string) {
	if len(args) < 2 || !strings.EqualFold(string(args[0]), "FIELDS") {
		return nil, "ERR Mandatory keyword FIELDS is missing or not at the right position"
	}
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, "ERR value is not an integer or out of range"
	}
	if n <= 0 {
		return nil, "ERR Parameter `numFields` should be greater than 0"
	}
	rest := int64(len(args) - 2)
	if n > rest {
		return nil, "ERR Parameter `numFields` is more than number of arguments"
	}
	if n < rest {
		return nil, "ERR syntax error"
	}
	return args[2:], ""
}

// expireFrom computes base + n*unit milliseconds, reporting false on
// overflow; Redis calls that an invalid expire time. n may be negative
// (a past EXAT or PXAT), base never is.
func expireFrom(base, n, unit int64) (int64, bool) {
	if n > math.MaxInt64/unit || n < math.MinInt64/unit {
		return 0, false
	}
	v := n * unit
	if v > math.MaxInt64-base {
		return 0, false
	}
	return base + v, true
}

func arityErr(reply []byte, cmd string) []byte {
	return AppendError(reply, fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd)))
}

func syntaxErr(reply []byte) []byte {
	return AppendError(reply, "ERR syntax error")
}

func invalidExpire(reply []byte, cmd string) []byte {
	return AppendError(reply, fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(cmd)))
}

func storeErr(reply []byte, err error) []byte {
	// ErrWrongType carries its full wire text, WRONGTYPE prefix and
	// all; everything else is an ERR.
	if errors.Is(err, ErrWrongType) {
		return AppendError(reply, err.Error())
	}
	return AppendError(reply, "ERR "+err.Error())
}

// hllErr maps the HLL layer's sentinels onto Redis's exact wire
// texts; anything else routes through storeErr.
func hllErr(reply []byte, err error) []byte {
	switch {
	case errors.Is(err, errNotHLL):
		return AppendError(reply, "WRONGTYPE Key is not a valid HyperLogLog string value.")
	case errors.Is(err, errCorruptHLL):
		return AppendError(reply, "INVALIDOBJ Corrupted HLL object detected")
	}
	return storeErr(reply, err)
}

// pfdebugCmd mirrors Redis's pfdebugCommand: the key resolves before
// the subcommand's own arity check, and every error text matches.
func (s *Server) pfdebugCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	sub := strings.ToLower(string(args[1]))
	v, ok, err := s.s.PfGet(ctx, args[2])
	if err != nil {
		return hllErr(reply, err)
	}
	if !ok {
		return AppendError(reply, "ERR The specified key does not exist")
	}
	switch sub {
	case "getreg":
		if v[4] == hllEncSparse {
			if _, _, err := s.s.PfToDense(ctx, args[2]); err != nil {
				return hllErr(reply, err)
			}
			if v, _, err = s.s.PfGet(ctx, args[2]); err != nil {
				return hllErr(reply, err)
			}
		}
		reply = AppendArray(reply, hllRegisters)
		regs := v[hllHdrSize:]
		for i := range hllRegisters {
			reply = AppendInt(reply, int64(hllDenseGet(regs, i)))
		}
		return reply
	case "decode":
		if v[4] != hllEncSparse {
			return AppendError(reply, "ERR HLL encoding is not sparse")
		}
		var out []byte
		p := hllHdrSize
		for p < len(v) {
			switch {
			case hllSparseIsZero(v[p]):
				out = fmt.Appendf(out, "z:%d ", hllSparseZeroLen(v[p]))
				p++
			case hllSparseIsXZero(v[p]):
				if p+1 >= len(v) {
					p = len(v)
					continue
				}
				out = fmt.Appendf(out, "Z:%d ", hllSparseXZeroLen(v[p], v[p+1]))
				p += 2
			default:
				out = fmt.Appendf(out, "v:%d,%d ", hllSparseValValue(v[p]), hllSparseValLen(v[p]))
				p++
			}
		}
		if len(out) > 0 {
			out = out[:len(out)-1]
		}
		return AppendBulk(reply, out)
	case "encoding":
		if v[4] == hllEncDense {
			return AppendSimple(reply, "dense")
		}
		return AppendSimple(reply, "sparse")
	case "todense":
		conv, _, err := s.s.PfToDense(ctx, args[2])
		if err != nil {
			return hllErr(reply, err)
		}
		if conv {
			return AppendInt(reply, 1)
		}
		return AppendInt(reply, 0)
	}
	return AppendError(reply, fmt.Sprintf("ERR Unknown PFDEBUG subcommand '%s'", args[1]))
}

// lcsCmd parses the LCS options and shapes the reply like Redis 8.8:
// a bulk string by default, an integer under LEN, and under IDX the
// RESP2 rendering of a two-entry map, a four-item array holding
// "matches" with the ranges in backtrack order and "len" with the
// total. Options are case-insensitive and order-free.
func (s *Server) lcsCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	var getLen, getIdx, withMatchLen bool
	var minMatchLen int64
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "IDX":
			getIdx = true
		case "LEN":
			getLen = true
		case "WITHMATCHLEN":
			withMatchLen = true
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return syntaxErr(reply)
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if n < 0 {
				n = 0
			}
			minMatchLen = n
			i++
		default:
			return syntaxErr(reply)
		}
	}
	if getLen && getIdx {
		return AppendError(reply, "ERR If you want both the length and indexes, please just use IDX.")
	}
	a, b, err := s.s.LcsRead(ctx, args[1], args[2])
	if err != nil {
		return storeErr(reply, err)
	}
	total, result, matches, err := lcsRun(a, b, getLen, getIdx, minMatchLen)
	if err != nil {
		switch {
		case errors.Is(err, errLcsTooLong):
			return AppendError(reply, "ERR String too long for LCS")
		case errors.Is(err, errLcsAlloc):
			return AppendError(reply, "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len")
		}
		return storeErr(reply, err)
	}
	switch {
	case getIdx:
		reply = AppendArray(reply, 4)
		reply = AppendBulk(reply, []byte("matches"))
		reply = AppendArray(reply, len(matches))
		for _, m := range matches {
			if withMatchLen {
				reply = AppendArray(reply, 3)
			} else {
				reply = AppendArray(reply, 2)
			}
			reply = AppendArray(reply, 2)
			reply = AppendInt(reply, int64(m.aStart))
			reply = AppendInt(reply, int64(m.aEnd))
			reply = AppendArray(reply, 2)
			reply = AppendInt(reply, int64(m.bStart))
			reply = AppendInt(reply, int64(m.bEnd))
			if withMatchLen {
				reply = AppendInt(reply, int64(m.aEnd-m.aStart+1))
			}
		}
		reply = AppendBulk(reply, []byte("len"))
		return AppendInt(reply, int64(total))
	case getLen:
		return AppendInt(reply, int64(total))
	default:
		return AppendBulk(reply, result)
	}
}

// parseBitOffset accepts what string2ll accepts, bounded to the bit
// offsets a value at the 512 MiB cap can hold, for SETBIT and GETBIT.
func parseBitOffset(a []byte) (int64, bool) {
	n, ok := parseCanonicalInt(a)
	if !ok || n < 0 || n>>3 >= MaxValueLen {
		return 0, false
	}
	return n, true
}

const bitfieldTypeErr = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."

// parseBitfieldOps turns BITFIELD's token stream into ops, validating
// everything before the first op executes, as Redis does: any bad
// token means no write happens. OVERFLOW applies to the subcommands
// after it; the default is WRAP. The non-empty return string is the
// exact error reply.
func (s *Server) parseBitfieldOps(args [][]byte, ro bool) ([]BitfieldOp, string) {
	ops := s.bfOps[:0]
	ovf := byte('w')
	for i := 0; i < len(args); {
		var kind byte
		switch tok := strings.ToUpper(string(args[i])); tok {
		case "GET":
			kind = 'g'
		case "SET":
			kind = 's'
		case "INCRBY":
			kind = 'i'
		case "OVERFLOW":
			if ro {
				return nil, "ERR BITFIELD_RO only supports the GET subcommand"
			}
			if i+1 >= len(args) {
				return nil, "ERR syntax error"
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "WRAP":
				ovf = 'w'
			case "SAT":
				ovf = 's'
			case "FAIL":
				ovf = 'f'
			default:
				return nil, "ERR Invalid OVERFLOW type specified"
			}
			i += 2
			continue
		default:
			return nil, "ERR syntax error"
		}
		if ro && kind != 'g' {
			return nil, "ERR BITFIELD_RO only supports the GET subcommand"
		}
		need := 3
		if kind == 'g' {
			need = 2
		}
		if i+need >= len(args) {
			return nil, "ERR syntax error"
		}
		signed, w, ok := parseBitfieldType(args[i+1])
		if !ok {
			return nil, bitfieldTypeErr
		}
		off, ok := parseBitfieldOffset(args[i+2], w)
		if !ok {
			return nil, "ERR bit offset is not an integer or out of range"
		}
		op := BitfieldOp{Kind: kind, Signed: signed, Bits: w, Ovf: ovf, Off: off}
		if kind != 'g' {
			arg, ok := parseCanonicalInt(args[i+3])
			if !ok {
				return nil, "ERR value is not an integer or out of range"
			}
			op.Arg = arg
		}
		ops = append(ops, op)
		i += need + 1
	}
	s.bfOps = ops
	return ops, ""
}

// parseBitfieldType reads i1..i64 or u1..u63, case-insensitive on the
// letter; the width goes through string2ll so "u08" fails like Redis.
func parseBitfieldType(a []byte) (signed bool, w uint8, ok bool) {
	if len(a) < 2 {
		return false, 0, false
	}
	switch a[0] {
	case 'i', 'I':
		signed = true
	case 'u', 'U':
	default:
		return false, 0, false
	}
	n, ok := parseCanonicalInt(a[1:])
	if !ok || n < 1 || n > 64 || (!signed && n > 63) {
		return false, 0, false
	}
	return signed, uint8(n), true
}

// parseBitfieldOffset reads a BITFIELD offset, resolving the '#'
// typed-index form, and bounds the field's last byte to the value
// cap.
func parseBitfieldOffset(a []byte, w uint8) (uint64, bool) {
	hash := false
	if len(a) > 0 && a[0] == '#' {
		hash = true
		a = a[1:]
	}
	n, ok := parseCanonicalInt(a)
	if !ok || n < 0 {
		return 0, false
	}
	off := uint64(n)
	if hash {
		if off > uint64(math.MaxInt64)/uint64(w) {
			return 0, false
		}
		off *= uint64(w)
	}
	if (off+uint64(w)-1)>>3 >= MaxValueLen {
		return 0, false
	}
	return off, true
}

// strSizeErr maps the ladder's value cap onto Redis's wording for the
// growing string commands (APPEND, SETRANGE).
func strSizeErr(reply []byte, err error) []byte {
	if errors.Is(err, ErrValueTooLong) {
		return AppendError(reply, "ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	}
	return storeErr(reply, err)
}
