package command

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// The property test drives the same random command sequence against the model
// oracle in model_test.go and a live in-process aki dispatcher, and asserts the
// replies match step for step. On a mismatch it shrinks the sequence to a
// minimal failing case and reports it. This is the model-based testing layer of
// doc 23 section 5.

// commandGen produces random commands over a small key, field, and member
// alphabet. A small alphabet makes interesting interactions common: overwrites,
// type conflicts, pop-to-empty, re-adds.
type commandGen struct {
	rng     *rand.Rand
	keys    []string
	fields  []string
	members []string
	vals    []string
}

func newCommandGen(seed int64) *commandGen {
	g := &commandGen{rng: rand.New(rand.NewSource(seed))}
	for i := range 8 {
		g.keys = append(g.keys, "k"+strconv.Itoa(i))
	}
	for i := range 4 {
		g.fields = append(g.fields, "f"+strconv.Itoa(i))
	}
	for i := range 5 {
		g.members = append(g.members, "m"+strconv.Itoa(i))
	}
	// A mix of plain and numeric values so INCR sometimes works and sometimes
	// errors on a non-integer string.
	g.vals = []string{"a", "b", "hello", "0", "1", "42", "-7", "x"}
	return g
}

func (g *commandGen) pick(s []string) string { return s[g.rng.Intn(len(s))] }

func (g *commandGen) next() []string {
	k := func() string { return g.pick(g.keys) }
	f := func() string { return g.pick(g.fields) }
	m := func() string { return g.pick(g.members) }
	v := func() string { return g.pick(g.vals) }
	idx := func() string { return strconv.Itoa(g.rng.Intn(11) - 5) } // -5..5

	switch g.rng.Intn(28) {
	case 0:
		return []string{"SET", k(), v()}
	case 1:
		return []string{"SETNX", k(), v()}
	case 2:
		return []string{"GET", k()}
	case 3:
		return []string{"APPEND", k(), v()}
	case 4:
		return []string{"STRLEN", k()}
	case 5:
		return []string{"INCR", k()}
	case 6:
		return []string{"DECR", k()}
	case 7:
		return []string{"INCRBY", k(), strconv.Itoa(g.rng.Intn(21) - 10)}
	case 8:
		return []string{"DEL", k()}
	case 9:
		return []string{"EXISTS", k()}
	case 10:
		return []string{"TYPE", k()}
	case 11:
		return append([]string{"RPUSH", k()}, v(), v())
	case 12:
		return append([]string{"LPUSH", k()}, v())
	case 13:
		return []string{"RPOP", k()}
	case 14:
		return []string{"LPOP", k()}
	case 15:
		return []string{"LLEN", k()}
	case 16:
		return []string{"LINDEX", k(), idx()}
	case 17:
		return []string{"LRANGE", k(), idx(), idx()}
	case 18:
		return []string{"HSET", k(), f(), v()}
	case 19:
		return []string{"HGET", k(), f()}
	case 20:
		return []string{"HDEL", k(), f()}
	case 21:
		return []string{"HLEN", k()}
	case 22:
		return []string{"HEXISTS", k(), f()}
	case 23:
		return []string{"HGETALL", k()}
	case 24:
		return []string{"SADD", k(), m()}
	case 25:
		return []string{"SREM", k(), m()}
	case 26:
		return []string{"SCARD", k()}
	default:
		return []string{"SMEMBERS", k()}
	}
}

// akiClient wraps a dispatcher and one offline connection for in-process calls.
type akiClient struct {
	d    *Dispatcher
	conn *networking.Conn
}

func newAkiClient(tb testing.TB) *akiClient {
	return &akiClient{d: newFuzzDispatcher(tb), conn: networking.NewOfflineConn()}
}

// do runs one command and returns its reply in canonical form.
func (c *akiClient) do(argv []string) any {
	c.conn.ResetOut()
	bargv := make([][]byte, len(argv))
	for i, a := range argv {
		bargv[i] = []byte(a)
	}
	c.d.Handle(c.conn, bargv)
	out := c.conn.OutBytes()
	v, _, err := resp.Decode(out, 0)
	if err != nil {
		return fmt.Sprintf("DECODE-ERROR:%v:%q", err, out)
	}
	return toCanon(v)
}

// toCanon converts a decoded RESP value into the same canonical shape the model
// produces: a string, an int64, nil, an errReply, or a []any.
func toCanon(v resp.RESPValue) any {
	switch v.Type {
	case resp.TypeSimpleString:
		return string(v.Str)
	case resp.TypeError, resp.TypeBulkError:
		return errReply{leadCode(v.Err)}
	case resp.TypeInteger:
		return v.Integer
	case resp.TypeBulkString, resp.TypeVerbatim:
		if v.IsNull {
			return nil
		}
		return string(v.Str)
	case resp.TypeNull:
		return nil
	case resp.TypeArray, resp.TypeSet, resp.TypePush:
		if v.IsNull {
			return nil
		}
		out := make([]any, len(v.Elems))
		for i, e := range v.Elems {
			out[i] = toCanon(e)
		}
		return out
	case resp.TypeMap:
		out := make([]any, 0, len(v.Map)*2)
		for _, kv := range v.Map {
			out = append(out, toCanon(kv[0]), toCanon(kv[1]))
		}
		return out
	case resp.TypeBool:
		return v.Bool
	case resp.TypeDouble:
		return v.Float
	}
	return fmt.Sprintf("UNHANDLED-TYPE:%c", byte(v.Type))
}

// leadCode returns the first whitespace-delimited token of an error message,
// which is the error code clients switch on.
func leadCode(msg string) string {
	if i := strings.IndexByte(msg, ' '); i >= 0 {
		return msg[:i]
	}
	return msg
}

func TestModelProperties(t *testing.T) {
	const stepsPerSeed = 5000
	for _, seed := range []int64{1, 2, 3} {
		gen := newCommandGen(seed)
		model := newModelDB()
		client := newAkiClient(t)

		history := make([][]string, 0, stepsPerSeed)
		for i := range stepsPerSeed {
			cmd := gen.next()
			history = append(history, cmd)

			want := normalize(cmd[0], model.exec(cmd))
			got := normalize(cmd[0], client.do(cmd))
			if !canonEqual(want, got) {
				minimal := shrink(t, history)
				t.Fatalf("seed %d step %d: command %v\n  model = %v\n  aki   = %v\nminimal failing sequence:\n%s",
					seed, i, cmd, want, got, formatSeq(minimal))
			}
		}
	}
}

// replayFails replays a command sequence against a fresh model and dispatcher and
// reports whether the replies diverge.
func replayFails(tb testing.TB, cmds [][]string) bool {
	model := newModelDB()
	client := newAkiClient(tb)
	for _, cmd := range cmds {
		want := normalize(cmd[0], model.exec(cmd))
		got := normalize(cmd[0], client.do(cmd))
		if !canonEqual(want, got) {
			return true
		}
	}
	return false
}

// shrink reduces a failing sequence to a minimal one: binary search for the
// shortest failing prefix, then drop individual commands that are not needed.
func shrink(tb testing.TB, history [][]string) [][]string {
	lo, hi := 1, len(history)
	for lo < hi {
		mid := (lo + hi) / 2
		if replayFails(tb, history[:mid]) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	minimal := append([][]string(nil), history[:lo]...)
	for i := 0; i < len(minimal); i++ {
		candidate := append(append([][]string(nil), minimal[:i]...), minimal[i+1:]...)
		if replayFails(tb, candidate) {
			minimal = candidate
			i--
		}
	}
	return minimal
}

func formatSeq(cmds [][]string) string {
	var b strings.Builder
	for _, c := range cmds {
		b.WriteString("  ")
		b.WriteString(strings.Join(c, " "))
		b.WriteByte('\n')
	}
	return b.String()
}
