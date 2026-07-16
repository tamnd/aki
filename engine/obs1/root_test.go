package obs1

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func sampleRoot() Root {
	r := Root{
		CreatedMS: 1_752_600_000_000,
		G:         64,
		D:         1,
		Flags:     1,
		CkptSeq:   7,
		CkptDD:    0,
		Settings:  []byte(`{"maxmemory":"0"}`),
	}
	copy(r.DBID[:], "0123456789abcdef")
	return r
}

func TestRootRoundTrip(t *testing.T) {
	for _, r := range []Root{sampleRoot(), {G: 1, D: 1}} {
		b := AppendRoot(nil, 42, r)
		got, h, err := ParseRoot(b)
		if err != nil {
			t.Fatalf("ParseRoot: %v", err)
		}
		if h.Format != FormatRoot || h.FVersion != 1 || h.Writer != 42 {
			t.Fatalf("header %+v", h)
		}
		if got.DBID != r.DBID || got.CreatedMS != r.CreatedMS || got.G != r.G ||
			got.D != r.D || got.Flags != r.Flags || got.CkptSeq != r.CkptSeq ||
			got.CkptDD != r.CkptDD || !bytes.Equal(got.Settings, r.Settings) {
			t.Fatalf("round trip: got %+v want %+v", got, r)
		}
	}
}

func TestRootRejects(t *testing.T) {
	good := AppendRoot(nil, 1, sampleRoot())

	flip := func(i int) []byte {
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		return b
	}
	cases := map[string][]byte{
		"truncated header":  good[:HeaderSize-1],
		"truncated payload": good[:HeaderSize+10],
		"missing crc":       good[:len(good)-1],
		"trailing byte":     append(append([]byte(nil), good...), 0),
		"payload bit flip":  flip(HeaderSize + 3),
		"crc bit flip":      flip(len(good) - 1),
		"settings len flip": flip(HeaderSize + rootFixed - 4),
	}
	crossType := AppendHeader(nil, Header{Format: FormatWAL, FVersion: 1, Writer: 1})
	crossType = append(crossType, good[HeaderSize:]...)
	cases["cross-typed WAL"] = crossType
	badVersion := AppendHeader(nil, Header{Format: FormatRoot, FVersion: 2, Writer: 1})
	badVersion = append(badVersion, good[HeaderSize:]...)
	cases["fversion 2"] = badVersion

	for name, b := range cases {
		if _, _, err := ParseRoot(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSeq16(t *testing.T) {
	if got := seq16(0); got != "0000000000000000" {
		t.Fatalf("seq16(0) = %q", got)
	}
	if got := seq16(42); got != "0000000000000042" {
		t.Fatalf("seq16(42) = %q", got)
	}
	if seq16(9) >= seq16(10) || seq16(99) >= seq16(100) {
		t.Fatal("lexicographic order does not match numeric order")
	}
}

func TestDBKey(t *testing.T) {
	if got := dbKey("", "root"); got != "root" {
		t.Fatalf("empty prefix: %q", got)
	}
	if got := dbKey("db/alpha", "rootv/"+seq16(1)); got != "db/alpha/rootv/0000000000000001" {
		t.Fatalf("prefixed: %q", got)
	}
}

func TestRootOlder(t *testing.T) {
	at := func(dd uint16, seq uint64) Root { return Root{CkptDD: dd, CkptSeq: seq} }
	if !rootOlder(at(0, 3), at(0, 4)) || rootOlder(at(0, 4), at(0, 4)) || rootOlder(at(0, 5), at(0, 4)) {
		t.Fatal("same-domain ordering wrong")
	}
	if !rootOlder(at(0, 99), at(1, 0)) || rootOlder(at(1, 0), at(0, 99)) {
		t.Fatal("domain must dominate seq")
	}
}

// scriptStore plays a canned sequence of Store responses so each ambiguity
// branch of the advance protocols can be pinned deterministically. The nil
// embedded Store makes any unscripted call panic, which is the point.
type scriptStore struct {
	Store
	t     *testing.T
	steps []scriptStep
}

type scriptStep struct {
	op   string // "get", "ifmatch", "ifabsent", "recheck"
	key  string
	body []byte
	info ObjectInfo
	out  RecheckOutcome
	err  error
}

func (s *scriptStore) next(op, key string) scriptStep {
	s.t.Helper()
	if len(s.steps) == 0 {
		s.t.Fatalf("unscripted call %s %q", op, key)
	}
	st := s.steps[0]
	s.steps = s.steps[1:]
	if st.op != op || st.key != key {
		s.t.Fatalf("call %s %q, script wants %s %q", op, key, st.op, st.key)
	}
	return st
}

func (s *scriptStore) Get(_ context.Context, key string) ([]byte, ObjectInfo, error) {
	st := s.next("get", key)
	return st.body, st.info, st.err
}

func (s *scriptStore) PutIfMatch(_ context.Context, key string, _ []byte, token string, _ WriteTag) (ObjectInfo, error) {
	st := s.next("ifmatch", key)
	if st.err == nil && token != st.info.ETag {
		s.t.Fatalf("PutIfMatch token %q, script staged %q", token, st.info.ETag)
	}
	return st.info, st.err
}

func (s *scriptStore) PutIfAbsent(_ context.Context, key string, _ []byte, _ WriteTag) (ObjectInfo, error) {
	st := s.next("ifabsent", key)
	return st.info, st.err
}

func (s *scriptStore) Recheck(_ context.Context, key string, _ WriteTag) (RecheckOutcome, []byte, ObjectInfo, error) {
	st := s.next("recheck", key)
	return st.out, st.body, st.info, st.err
}

func rootAt(seq uint64) Root {
	r := sampleRoot()
	r.CkptSeq = seq
	return r
}

func TestAdvanceRootScripts(t *testing.T) {
	enc := func(writer uint64, seq uint64) []byte { return AppendRoot(nil, writer, rootAt(seq)) }

	t.Run("cas ambiguous applied converges by re-read", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "root", body: enc(0, 0), info: ObjectInfo{ETag: "e1"}},
			{op: "ifmatch", key: "root", info: ObjectInfo{ETag: "e1"}, err: ErrAmbiguous},
			{op: "get", key: "root", body: enc(7, 5), info: ObjectInfo{ETag: "e2"}},
		}}
		if err := AdvanceRoot(t.Context(), s, "", false, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cas 412 against a newer root is a clean stop", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "root", body: enc(0, 0), info: ObjectInfo{ETag: "e1"}},
			{op: "ifmatch", key: "root", info: ObjectInfo{ETag: "e1"}, err: ErrPrecondition},
			{op: "get", key: "root", body: enc(9, 99), info: ObjectInfo{ETag: "e3"}},
		}}
		if err := AdvanceRoot(t.Context(), s, "", false, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fallback ambiguous resolved ours", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "rootv/" + seq16(0), body: enc(0, 0)},
			{op: "get", key: "rootv/" + seq16(1), err: ErrNotFound},
			{op: "ifabsent", key: "rootv/" + seq16(1), err: ErrAmbiguous},
			{op: "recheck", key: "rootv/" + seq16(1), out: RecheckOurs},
		}}
		if err := AdvanceRoot(t.Context(), s, "", true, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fallback ambiguous resolved absent retries the create", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "rootv/" + seq16(0), body: enc(0, 0)},
			{op: "get", key: "rootv/" + seq16(1), err: ErrNotFound},
			{op: "ifabsent", key: "rootv/" + seq16(1), err: ErrAmbiguous},
			{op: "recheck", key: "rootv/" + seq16(1), out: RecheckAbsent},
			{op: "ifabsent", key: "rootv/" + seq16(1)},
		}}
		if err := AdvanceRoot(t.Context(), s, "", true, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fallback ambiguous resolved theirs, newer, stop", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "rootv/" + seq16(0), body: enc(0, 0)},
			{op: "get", key: "rootv/" + seq16(1), err: ErrNotFound},
			{op: "ifabsent", key: "rootv/" + seq16(1), err: ErrAmbiguous},
			{op: "recheck", key: "rootv/" + seq16(1), out: RecheckOther, body: enc(9, 99)},
		}}
		if err := AdvanceRoot(t.Context(), s, "", true, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fallback ambiguous resolved theirs, older, take the next slot", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "rootv/" + seq16(0), body: enc(0, 0)},
			{op: "get", key: "rootv/" + seq16(1), err: ErrNotFound},
			{op: "ifabsent", key: "rootv/" + seq16(1), err: ErrAmbiguous},
			{op: "recheck", key: "rootv/" + seq16(1), out: RecheckOther, body: enc(9, 3)},
			{op: "ifabsent", key: "rootv/" + seq16(2)},
		}}
		if err := AdvanceRoot(t.Context(), s, "", true, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fallback lost create reads the winner and re-decides", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "rootv/" + seq16(0), body: enc(0, 0)},
			{op: "get", key: "rootv/" + seq16(1), err: ErrNotFound},
			{op: "ifabsent", key: "rootv/" + seq16(1), err: ErrPrecondition},
			{op: "get", key: "rootv/" + seq16(1), body: enc(9, 3)},
			{op: "ifabsent", key: "rootv/" + seq16(2)},
		}}
		if err := AdvanceRoot(t.Context(), s, "", true, 7, rootAt(5)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("advancing an empty fallback sequence is ErrNotFound", func(t *testing.T) {
		s := &scriptStore{t: t, steps: []scriptStep{
			{op: "get", key: "rootv/" + seq16(0), err: ErrNotFound},
		}}
		err := AdvanceRoot(t.Context(), s, "", true, 7, rootAt(5))
		if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "no root") {
			t.Fatalf("err = %v", err)
		}
	})
}

func FuzzParseRoot(f *testing.F) {
	f.Add(AppendRoot(nil, 1, sampleRoot()))
	f.Add(AppendRoot(nil, 0, Root{G: 1, D: 1}))
	noSettings := sampleRoot()
	noSettings.Settings = nil
	f.Add(AppendRoot(nil, 1<<63, noSettings))
	good := AppendRoot(nil, 7, sampleRoot())
	f.Add(good[:HeaderSize])
	f.Add(good[:len(good)-1])
	f.Add(append(append([]byte(nil), good...), 0))
	for _, off := range []int{0, 16, HeaderSize, HeaderSize + rootFixed - 4, len(good) - 1} {
		b := append([]byte(nil), good...)
		b[off] ^= 0x80
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r, h, err := ParseRoot(b)
		if err != nil {
			return
		}
		if !bytes.Equal(AppendRoot(nil, h.Writer, r), b) {
			t.Fatalf("accepted bytes do not re-encode to the input")
		}
	})
}
