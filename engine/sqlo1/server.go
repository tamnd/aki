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
	t *Tiered
	s *Str

	mu sync.Mutex // serializes command execution against the runtime

	// now is the clock, swappable by tests that exercise expiry.
	now func() int64 // wall milliseconds

	// old carries a reply value across the mutation that would recycle
	// its arena bytes (SET GET, GETDEL, GETEX).
	old []byte

	// mkeys and mvals split MSET and MSETNX argument pairs.
	mkeys [][]byte
	mvals [][]byte
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
	t := NewTiered(st, TieredConfig{
		Budget: ComputeBudget(serverMemCap, 1),
		NowMs:  func() int64 { return srv.now() },
	})
	str, err := NewStr(t, StrConfig{})
	if err != nil {
		return nil, err
	}
	srv.t, srv.s = t, str
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
	case "TYPE":
		if len(args) != 2 {
			return arityErr(reply, cmd)
		}
		exists, _, err := s.s.Entry(ctx, args[1])
		if err != nil {
			return storeErr(reply, err)
		}
		if !exists {
			return AppendSimple(reply, "none")
		}
		// The only type in T1; the answer routes through the header tag
		// when the collection types land.
		return AppendSimple(reply, "string")
	case "OBJECT":
		if len(args) == 3 && strings.EqualFold(string(args[1]), "ENCODING") {
			enc, ok, err := s.s.Encoding(ctx, args[2])
			if err != nil {
				return storeErr(reply, err)
			}
			if !ok {
				return AppendError(reply, "ERR no such key")
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
	return AppendError(reply, "ERR "+err.Error())
}

// strSizeErr maps the ladder's value cap onto Redis's wording for the
// growing string commands (APPEND, SETRANGE).
func strSizeErr(reply []byte, err error) []byte {
	if errors.Is(err, ErrValueTooLong) {
		return AppendError(reply, "ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	}
	return storeErr(reply, err)
}
