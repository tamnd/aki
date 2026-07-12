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
// under the default config (spec 2064/f3/10 section 4.4):
//
//	hash-max-listpack-entries 128
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
	{"HSET", "h", "odd", "1", "trailing"},       // arity error (handler-enforced)
	{"HMSET", "h", "odd", "1", "trailing"},      // arity error (handler-enforced)
	{"OBJECT", "ENCODING", "h"},                 // $listpack
	{"OBJECT", "ENCODING", "s"},                 // $int (string store, via the chain)
	{"OBJECT", "ENCODING", "gone"},              // -ERR no such key
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
}

// verbOp routes a script verb to the harness op byte.
var verbOp = map[string]byte{
	"HSET": opHset, "HMSET": opHmset, "HSETNX": opHsetnx,
	"HGET": opHget, "HMGET": opHmget, "HDEL": opHdel,
	"HEXISTS": opHexists, "HLEN": opHlen, "HSTRLEN": opHstrlen,
	"OBJECT": opObject, "SET": opSet,
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
	rc.del("s", "h", "nokey", "gone")

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
