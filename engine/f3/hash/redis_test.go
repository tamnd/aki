package hash

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The live Redis differential (spec 2064/f3/10 section 7). Two suites replay the
// same work against a real server when AKI_REDIS_ADDR points at one, so a Redis
// version bump that moves a band threshold or changes a reply shape surfaces as a
// failure here rather than as silent parity drift:
//
//	TestHashEncodingAgainstRedis  the band ladder: OBJECT ENCODING after a build
//	TestPointOpsAgainstRedis      every point verb, reply for reply, incl. the
//	                              listpack->hashtable transition mid-script
//
// Both are skipped by default; they are the confirmation lever, not a required
// gate. The default path is the in-process conversion differential and the
// command harness, which need no server.
//
// The client speaks just enough RESP to send an inline-array command and read one
// reply of any type into a canonical string. The aki side renders its raw reply
// bytes through the same canonical reader, so the two strings compare directly and
// no dependency is added.

// --- a throwaway RESP client --------------------------------------------------

type redisConn struct {
	c net.Conn
	r *bufio.Reader
}

func dialRedis(addr string) (*redisConn, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &redisConn{c: c, r: bufio.NewReader(c)}, nil
}

func (rc *redisConn) close() { rc.c.Close() }

// send writes an inline-array command; the caller reads the reply.
func (rc *redisConn) send(args ...string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	rc.c.SetDeadline(time.Now().Add(2 * time.Second))
	_, err := rc.c.Write([]byte(b.String()))
	return err
}

// canon sends a command and returns its reply in the canonical form below.
func (rc *redisConn) canon(args ...string) (string, error) {
	if err := rc.send(args...); err != nil {
		return "", err
	}
	return canonRead(rc.r)
}

// del is a best-effort reset that ignores its reply, used to clear scratch keys.
func (rc *redisConn) del(keys ...string) {
	rc.send(append([]string{"DEL"}, keys...)...)
	canonRead(rc.r)
}

// --- one canonical reply reader shared by both sides --------------------------
//
// The canonical form collapses a RESP reply to a single comparable string:
//
//	+OK        status          -ERR ...   error
//	:5         integer         $hello     bulk string    $-1  nil bulk
//	*2[a,b]    array           *-1        nil array
//
// Both the live socket and the aki reply bytes are decoded by this one function,
// so a match means the two servers agree on type, framing, and payload.

func canonRead(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("empty reply")
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "$-1", nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			return "", err
		}
		return "$" + string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "*-1", nil
		}
		parts := make([]string, n)
		for i := 0; i < n; i++ {
			if parts[i], err = canonRead(r); err != nil {
				return "", err
			}
		}
		return "*" + strconv.Itoa(n) + "[" + strings.Join(parts, ",") + "]", nil
	default:
		return "", fmt.Errorf("unexpected reply %q", line)
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// canonHarness renders one aki raw reply through the same reader as the live side.
func canonHarness(t *testing.T, raw []byte) string {
	t.Helper()
	s, err := canonRead(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		t.Fatalf("decode aki reply %q: %v", raw, err)
	}
	return s
}

// --- the vendored band ladder, replayed live ---------------------------------

// hashEncodingCases carry the encoding real Redis reports for the same fields
// under the redis 8.8.0 build defaults (spec 2064/f3/10 section 4.4), verified
// live: at 512 fields OBJECT ENCODING is listpack and at 513 it flips to
// hashtable, and a 64-byte field or value stays listpack while 65 bytes converts:
//
//	hash-max-listpack-entries 512
//	hash-max-listpack-value   64
//
// The expectations are vendored so TestHashEncodingParity runs offline;
// TestHashEncodingAgainstRedis replays the same table against a live server.
var hashEncodingCases = []struct {
	name   string
	fields [][2]string
	want   string
}{
	{"few fields", pairsList(3), "listpack"},
	{"at entry cap", pairsList(maxListpackEntries), "listpack"},
	{"over entry cap", pairsList(maxListpackEntries + 1), "hashtable"},
	{"value at cap", [][2]string{{"f", strings.Repeat("a", maxListpackValue)}}, "listpack"},
	{"value over cap", [][2]string{{"f", strings.Repeat("a", maxListpackValue+1)}}, "hashtable"},
	{"field at cap", [][2]string{{strings.Repeat("k", maxListpackValue), "v"}}, "listpack"},
	{"field over cap", [][2]string{{strings.Repeat("k", maxListpackValue+1), "v"}}, "hashtable"},
}

func pairsList(n int) [][2]string {
	out := make([][2]string, n)
	for i := range out {
		out[i] = [2]string{"f" + strconv.Itoa(i), "v" + strconv.Itoa(i)}
	}
	return out
}

// buildHash seats fields on a fresh hash in order, the way HSET builds one field
// at a time, so the conversion trigger fires exactly where a live server's would.
func buildHash(fields [][2]string) *hash {
	h := newHash()
	for _, p := range fields {
		h.set([]byte(p[0]), []byte(p[1]))
	}
	return h
}

func TestHashEncodingParity(t *testing.T) {
	for _, tc := range hashEncodingCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildHash(tc.fields).enc.String(); got != tc.want {
				t.Fatalf("encoding = %q, Redis reports %q for %d fields", got, tc.want, len(tc.fields))
			}
		})
	}
}

func TestHashEncodingAgainstRedis(t *testing.T) {
	rc := dialForDiff(t)
	defer rc.close()
	for _, tc := range hashEncodingCases {
		key := "aki:hash:enc:" + tc.name
		rc.del(key)
		args := []string{"HSET", key}
		for _, p := range tc.fields {
			args = append(args, p[0], p[1])
		}
		if _, err := rc.canon(args...); err != nil {
			t.Fatalf("%s: HSET: %v", tc.name, err)
		}
		got, err := rc.canon("OBJECT", "ENCODING", key)
		if err != nil {
			t.Fatalf("%s: OBJECT ENCODING: %v", tc.name, err)
		}
		if got != "$"+tc.want {
			t.Errorf("%s: live Redis encoding %q, vendored table says %q", tc.name, got, "$"+tc.want)
		}
		rc.del(key)
	}
}

// --- the point-command differential ------------------------------------------

// pointScript is a fixed sequence of point commands that walks a hash through the
// band boundary and touches every verb this slice ships, including the reply
// shapes that are easy to get wrong: the HMGET array with an embedded nil, the
// empty-value field, the last-field-drop that removes the key, and OBJECT ENCODING
// before and after promotion. aki and Redis must answer each line identically.
var pointScript = [][]string{
	{"SET", "s", "v"},                           // seed a string key for the WRONGTYPE checks
	{"HSET", "s", "a", "1"},                     // WRONGTYPE
	{"HGET", "s", "a"},                          // WRONGTYPE
	{"HDEL", "s", "a"},                          // WRONGTYPE
	{"HLEN", "s"},                               // WRONGTYPE
	{"HSET", "h", "a", "1", "b", "2", "c", "3"}, // 3 new
	{"HSET", "h", "a", "9", "d", "4"},           // 1 new, 1 overwrite -> 1
	{"HGET", "h", "a"},                          // $9
	{"HGET", "h", "missing"},                    // $-1
	{"HGET", "nokey", "a"},                      // $-1
	{"HLEN", "h"},                               // :4
	{"HSET", "h", "e", ""},                      // empty value, new -> :1
	{"HGET", "h", "e"},                          // $ (empty bulk)
	{"HSTRLEN", "h", "e"},                       // :0
	{"HSTRLEN", "h", "a"},                       // :1
	{"HSTRLEN", "h", "missing"},                 // :0
	{"HEXISTS", "h", "a"},                       // :1
	{"HEXISTS", "h", "zzz"},                     // :0
	{"HMGET", "h", "a", "zzz", "b", "a"},        // *4[$9,$-1,$2,$9]
	{"HMGET", "nokey", "a", "b"},                // *2[$-1,$-1]
	{"HSETNX", "h", "a", "0"},                   // exists -> :0
	{"HSETNX", "h", "fresh", "7"},               // new -> :1
	{"HMSET", "h", "x", "10", "y", "20"},        // +OK
	// Arithmetic verbs: a missing field starts at zero, the rendering rides back.
	{"HINCRBY", "h", "cnt", "5"},            // new field -> :5
	{"HINCRBY", "h", "cnt", "-2"},           // :3
	{"HINCRBY", "h", "a", "1"},              // "9" + 1 -> :10
	{"HINCRBYFLOAT", "h", "cnt", "0.5"},     // "3" + 0.5 -> $3.5
	{"HINCRBYFLOAT", "h", "f", "5.0e3"},     // new -> $5000
	{"HINCRBY", "h", "e", "1"},              // "" empty field is not an int -> err
	{"HINCRBYFLOAT", "h", "f", "notafloat"}, // err, unchanged
	{"HINCRBYFLOAT", "h", "f", "inf"},       // infinite increment -> "value is NaN or Infinity"
	{"HSET", "h", "huge", "1e308"},          // :1
	{"HINCRBYFLOAT", "h", "huge", "1e308"},  // sum overflows -> "increment would produce ..."
	{"HSET", "h", "odd", "1", "trailing"},   // arity error (handler-enforced)
	{"HMSET", "h", "odd", "1", "trailing"},  // arity error (handler-enforced)
	{"OBJECT", "ENCODING", "h"},             // $listpack
	{"OBJECT", "ENCODING", "s"},             // $int (string store, via the chain)
	{"OBJECT", "ENCODING", "gone"},          // $-1 nil (redis 8.8.0, not an error)
	// Promote by writing a value past the 64-byte cap, then re-probe the same
	// fields on the native side to prove the boundary is invisible.
	{"HSET", "h", "wide", strings.Repeat("z", maxListpackValue+1)}, // :1, promotes
	{"OBJECT", "ENCODING", "h"},                                    // $hashtable
	{"HGET", "h", "a"},                                             // $9
	{"HGET", "h", "wide"},                                          // the wide value
	{"HMGET", "h", "a", "zzz", "b"},                                // *3[$9,$-1,$2]
	{"HLEN", "h"},                                                  // native card
	// Drain the key one field at a time; the last HDEL drops it.
	{"HDEL", "h", "a", "zzz", "b"}, // :2 (zzz absent)
	{"HDEL", "nokey", "a"},         // :0

	// HSCAN over a fresh inline hash. On the listpack band Redis returns the whole
	// hash in one page with cursor 0 in insertion order, so every one of these is
	// byte-comparable; the native band's cursor order and HRANDFIELD's randomness
	// are not, and live in the structural tests instead.
	{"HSET", "hs", "user:1", "a", "user:2", "b", "post:1", "c"}, // :3
	{"HSCAN", "hs", "0"},                                // *2[$0,*6[pairs, insertion order]]
	{"HSCAN", "hs", "0", "COUNT", "100"},                // COUNT is a hint: same full page
	{"HSCAN", "hs", "0", "NOVALUES"},                    // *2[$0,*3[fields only]]
	{"HSCAN", "hs", "0", "MATCH", "user:*"},             // *2[$0,*4[the two user pairs]]
	{"HSCAN", "hs", "0", "MATCH", "user:*", "NOVALUES"}, // filtered, fields only
	{"HSCAN", "hs", "99"},                               // listpack ignores the cursor -> full page, cursor 0
	{"HSCAN", "missingkey", "0"},                        // missing key -> *2[$0,*0[]]
	// HSCAN argument errors, byte for byte.
	{"HSCAN", "hs", "notacursor"},      // -ERR invalid cursor
	{"HSCAN", "hs", "0", "BOGUS"},      // -ERR syntax error
	{"HSCAN", "hs", "0", "COUNT", "0"}, // -ERR syntax error (COUNT must be positive)
	{"HSCAN", "hs", "0", "MATCH"},      // -ERR syntax error (dangling MATCH)
	{"HSCAN", "s", "0"},                // WRONGTYPE (string key)

	// HRANDFIELD deterministic paths only: the nil, the empty array, and the
	// argument errors. The actual draws are random, so they are not replayed here.
	{"HRANDFIELD", "nokey"},          // no count, absent key -> $-1
	{"HRANDFIELD", "nokey", "5"},     // count, absent key -> *0[]
	{"HRANDFIELD", "hs", "0"},        // count 0 -> *0[]
	{"HRANDFIELD", "hs", "notanint"}, // -ERR value is not an integer or out of range
	{"HRANDFIELD", "hs", "1", "FOO"}, // bad third token -> -ERR syntax error
	{"HRANDFIELD", "s"},              // WRONGTYPE (string key)

	// HGETALL/HKEYS/HVALS on the inline hash. The listpack band keeps insertion
	// order, so the whole reply is byte-comparable; the native band's draw-vector
	// order is not and lives in the structural tests instead.
	{"HGETALL", "hs"},         // *6[user:1,a,user:2,b,post:1,c] insertion order
	{"HKEYS", "hs"},           // *3[user:1,user:2,post:1]
	{"HVALS", "hs"},           // *3[a,b,c]
	{"HGETALL", "missingkey"}, // missing key -> *0[]
	{"HKEYS", "missingkey"},   // *0[]
	{"HVALS", "missingkey"},   // *0[]
	{"HGETALL", "s"},          // WRONGTYPE (string key)
	{"HKEYS", "s"},            // WRONGTYPE
	{"HVALS", "s"},            // WRONGTYPE

	// --- field TTL: the HEXPIRE family (spec 2064/f3/10 section 6) -------------
	// Absolute-time setters and the expiretime queries are byte-comparable; the
	// relative HTTL/HPTTL are clock-relative and live in the fixed-clock structural
	// tests, except the fixed -2/-1 codes, which ride here. The absolute times are
	// far enough out (year 2191) that a future expiry stays future and its stored
	// value rounds back exactly, so every reply below is deterministic.
	{"HEXPIRE", "nokey", "100", "FIELDS", "2", "a", "b"},         // missing key -> *2[:-2,:-2]
	{"HTTL", "nokey", "FIELDS", "1", "a"},                        // *1[:-2]
	{"HPERSIST", "nokey", "FIELDS", "1", "a"},                    // *1[:-2]
	{"HSET", "hx", "a", "1", "b", "2", "c", "3"},                 // :3, inline
	{"OBJECT", "ENCODING", "hx"},                                 // $listpack, no TTL yet
	{"HEXPIREAT", "hx", "7000000000", "FIELDS", "2", "a", "zzz"}, // *2[:1,:-2]
	{"OBJECT", "ENCODING", "hx"},                                 // $listpackex once a TTL rode in
	{"HEXPIRETIME", "hx", "FIELDS", "3", "a", "b", "zzz"},        // *3[:7000000000,:-1,:-2]
	{"HTTL", "hx", "FIELDS", "1", "b"},                           // present, no TTL -> *1[:-1]
	// Conditions on the absolute setter, byte for byte.
	{"HEXPIREAT", "hx", "7000000000", "NX", "FIELDS", "1", "a"}, // has TTL -> :0
	{"HEXPIREAT", "hx", "8000000000", "XX", "FIELDS", "1", "a"}, // has TTL -> :1
	{"HEXPIREAT", "hx", "7000000000", "GT", "FIELDS", "1", "a"}, // 7e9<8e9 -> :0
	{"HEXPIREAT", "hx", "9000000000", "GT", "FIELDS", "1", "a"}, // 9e9>8e9 -> :1
	{"HEXPIREAT", "hx", "9500000000", "LT", "FIELDS", "1", "a"}, // 9.5e9>9e9 -> :0
	{"HEXPIREAT", "hx", "8500000000", "LT", "FIELDS", "1", "a"}, // 8.5e9<9e9 -> :1
	{"HEXPIREAT", "hx", "7000000000", "NX", "FIELDS", "1", "c"}, // no TTL -> :1
	{"HEXPIREAT", "hx", "7000000000", "GT", "FIELDS", "1", "b"}, // no TTL, GT refuses -> :0
	{"HPERSIST", "hx", "FIELDS", "3", "a", "c", "zzz"},          // *3[:1,:1,:-2]
	{"HEXPIRETIME", "hx", "FIELDS", "1", "a"},                   // *1[:-1] after persist
	// HSET clears a field TTL, HINCRBY preserves it.
	{"HEXPIREAT", "hx", "7000000000", "FIELDS", "1", "a"}, // :1
	{"HSET", "hx", "a", "99"},                             // overwrite -> :0
	{"HEXPIRETIME", "hx", "FIELDS", "1", "a"},             // *1[:-1] cleared
	{"HSET", "hx", "n", "10"},                             // :1
	{"HEXPIREAT", "hx", "7000000000", "FIELDS", "1", "n"}, // :1
	{"HINCRBY", "hx", "n", "5"},                           // :15
	{"HEXPIRETIME", "hx", "FIELDS", "1", "n"},             // *1[:7000000000] preserved
	// Millisecond absolute round-trip.
	{"HPEXPIREAT", "hx", "7000000000500", "FIELDS", "1", "b"}, // :1
	{"HPEXPIRETIME", "hx", "FIELDS", "1", "b"},                // *1[:7000000000500]
	// Set-to-the-past deletes the field on the spot.
	{"HEXPIREAT", "hx", "1", "FIELDS", "1", "c"}, // past -> :2
	{"HEXISTS", "hx", "c"},                       // :0
	// Native band still reports hashtable with a field TTL.
	{"HSET", "hn", "a", "1"}, // :1
	{"HSET", "hn", "wide", strings.Repeat("z", maxListpackValue+1)}, // :1, promotes
	{"HEXPIREAT", "hn", "7000000000", "FIELDS", "1", "a"},           // :1
	{"OBJECT", "ENCODING", "hn"},                                    // $hashtable
	{"HEXPIRETIME", "hn", "FIELDS", "1", "a"},                       // *1[:7000000000]
	// Error paths outrank the key lookup, byte for byte.
	{"HEXPIRE", "hx", "-5", "FIELDS", "1", "a"},                   // must be >= 0
	{"HEXPIRE", "hx", "99999999999999999", "FIELDS", "1", "a"},    // over cap
	{"HPEXPIREAT", "hx", "99999999999999999", "FIELDS", "1", "a"}, // over cap
	{"HEXPIRE", "hx", "notanint", "FIELDS", "1", "a"},             // not an integer
	{"HEXPIRE", "hx", "100", "FIELDS", "0", "a"},                  // numFields 0
	{"HEXPIRE", "hx", "100", "FIELDS", "xx", "a"},                 // numFields not int
	{"HEXPIRE", "hx", "100", "FIELDS", "3", "a"},                  // short count
	{"HEXPIRE", "hx", "100", "FIELDS", "1", "a", "b"},             // long count -> unknown argument: b
	{"HEXPIRE", "hx", "100", "a"},                                 // no FIELDS
	{"HEXPIRE", "hx", "100", "ZZ", "FIELDS", "1", "a"},            // unknown token before FIELDS
	{"HEXPIRE", "hx", "100", "NX", "XX", "FIELDS", "1", "a"},      // multiple conditions
	{"HEXPIRE", "s", "100", "FIELDS", "1", "a"},                   // WRONGTYPE
	{"HTTL", "s", "FIELDS", "1", "a"},                             // WRONGTYPE
	{"HPERSIST", "s", "FIELDS", "1", "a"},                         // WRONGTYPE
	{"HEXPIRETIME", "s", "FIELDS", "1", "a"},                      // WRONGTYPE
}

// verbOp routes a script verb to the harness op byte.
var verbOp = map[string]byte{
	"HSET": opHset, "HMSET": opHmset, "HSETNX": opHsetnx,
	"HGET": opHget, "HMGET": opHmget, "HDEL": opHdel,
	"HEXISTS": opHexists, "HLEN": opHlen, "HSTRLEN": opHstrlen,
	"HINCRBY": opHincrby, "HINCRBYFLOAT": opHincrbyfloat,
	"HSCAN": opHscan, "HRANDFIELD": opHrandfield,
	"HGETALL": opHgetall, "HKEYS": opHkeys, "HVALS": opHvals,
	"HEXPIRE": opHexpire, "HPEXPIRE": opHpexpire,
	"HEXPIREAT": opHexpireat, "HPEXPIREAT": opHpexpireat,
	"HTTL": opHttl, "HPTTL": opHpttl,
	"HEXPIRETIME": opHexpiretime, "HPEXPIRETIME": opHpexpiretime,
	"HPERSIST": opHpersist,
	"OBJECT":   opObject, "SET": opSet,
}

// runHarness dispatches one script command through the in-process handlers.
func runHarness(t *testing.T, c *shard.Conn, cmd []string) []byte {
	t.Helper()
	op, ok := verbOp[cmd[0]]
	if !ok {
		t.Fatalf("script verb %q not wired into the harness", cmd[0])
	}
	keyIdx := 0
	if cmd[0] == "OBJECT" { // OBJECT ENCODING <key>: the key is args[1]
		keyIdx = 1
	}
	return doAt(t, c, op, keyIdx, cmd[1:]...)
}

func TestPointOpsAgainstRedis(t *testing.T) {
	rc := dialForDiff(t)
	defer rc.close()
	// Clear every key the script writes so a rerun starts clean.
	rc.del("s", "h", "nokey", "gone", "hs", "missingkey", "hx", "hn")

	hc := newHarness(t).NewConn()
	for i, cmd := range pointScript {
		want, err := rc.canon(cmd...)
		if err != nil {
			t.Fatalf("step %d %v: redis: %v", i, cmd, err)
		}
		got := canonHarness(t, runHarness(t, hc, cmd))
		if got != want {
			t.Fatalf("step %d %v: aki %q, redis %q", i, cmd, got, want)
		}
	}
}

// dialForDiff skips the suite when no server is configured, then dials it.
func dialForDiff(t *testing.T) *redisConn {
	t.Helper()
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to run the live Redis differential")
	}
	rc, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return rc
}
