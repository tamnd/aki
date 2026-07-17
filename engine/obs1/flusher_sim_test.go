package obs1_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

type sinkCall struct {
	walSeq uint64
	size   int64
	index  []obs1.WALIndexEntry
}

// chanSink records deliveries on a channel wide enough that it never
// blocks a test's flusher.
type chanSink struct {
	ch  chan sinkCall
	err error
}

func newChanSink() *chanSink {
	return &chanSink{ch: make(chan sinkCall, 256)}
}

func (s *chanSink) WALFlushed(walSeq uint64, size int64, index []obs1.WALIndexEntry) error {
	s.ch <- sinkCall{walSeq: walSeq, size: size, index: index}
	return s.err
}

func waitCall(t *testing.T, s *chanSink) sinkCall {
	t.Helper()
	select {
	case c := <-s.ch:
		return c
	case <-time.After(10 * time.Second):
		t.Fatalf("no sink delivery within 10s")
		return sinkCall{}
	}
}

func opFrame(t *testing.T, slot uint16, seq uint64, key string, op obs1.Op) obs1.WALFrame {
	t.Helper()
	f, err := obs1.EncodeOp(slot, seq, []byte(key), op)
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	return f
}

func TestFlusherSizeTrigger(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 0xAB,
		FlushSize: 1024, FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	seq := uint64(0)
	val := make([]byte, 200)
	for i := range 8 {
		seq++
		if err := fl.AppendOp(1, 7, opFrame(t, 3, seq, "k", obs1.StrSet{Value: val})); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	c := waitCall(t, snk)
	if c.walSeq != 1 {
		t.Fatalf("first WAL seq = %d, want 1", c.walSeq)
	}
	key := fmt.Sprintf("p/wal/%016x/%016d", 0xAB, 1)
	body, _, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	if int64(len(body)) != c.size {
		t.Fatalf("object is %d bytes, sink said %d", len(body), c.size)
	}
	secs, hdr, err := obs1.ParseWAL(body)
	if err != nil {
		t.Fatalf("ParseWAL: %v", err)
	}
	if hdr.Writer != 0xAB {
		t.Fatalf("writer = %#x, want 0xab", hdr.Writer)
	}
	if len(secs) != 1 || secs[0].Group != 1 || secs[0].Epoch != 7 {
		t.Fatalf("sections = %+v, want one group 1 epoch 7", secs)
	}
	for _, f := range secs[0].Frames {
		if _, err := obs1.DecodeOp(f); err != nil {
			t.Fatalf("DecodeOp seq %d: %v", f.Seq, err)
		}
	}
}

func TestFlusherAgeTrigger(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 1,
		FlushAge: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	if err := fl.AppendOp(0, 1, opFrame(t, 0, 1, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	c := waitCall(t, snk)
	if c.walSeq != 1 || c.index[0].NFrames != 1 {
		t.Fatalf("delivery = %+v, want WAL 1 with one frame", c)
	}
}

func TestFlusherBarrier(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 1,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	if err := fl.AppendOp(0, 1, opFrame(t, 0, 1, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	fl.Barrier()
	waitCall(t, snk)
	st := fl.Stats()
	if st.Flushes != 1 || st.BarrierFlushes != 1 {
		t.Fatalf("stats = %+v, want one flush, one barrier flush", st)
	}
}

func TestFlusherMultiGroupObject(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 2,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	if err := fl.AppendOp(3, 9, opFrame(t, 30, 100, "c", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	if err := fl.AppendStrSet(1, 9, 10, 50, []byte("a"), []byte("v"), 123, obs1.LadderCounter); err != nil {
		t.Fatal(err)
	}
	if err := fl.AppendOp(2, 9, opFrame(t, 20, 70, "b", obs1.Expire{ExpiryMS: 5})); err != nil {
		t.Fatal(err)
	}
	fl.Barrier()
	c := waitCall(t, snk)
	body, _, err := store.Get(context.Background(), fmt.Sprintf("p/wal/%016x/%016d", 2, 1))
	if err != nil {
		t.Fatal(err)
	}
	secs, _, err := obs1.ParseWAL(body)
	if err != nil {
		t.Fatalf("ParseWAL: %v", err)
	}
	if len(secs) != 3 {
		t.Fatalf("got %d sections, want 3", len(secs))
	}
	for i, want := range []uint16{1, 2, 3} {
		if secs[i].Group != want {
			t.Fatalf("section %d group = %d, sections must sort by group", i, secs[i].Group)
		}
		if secs[i].Epoch != 9 {
			t.Fatalf("section %d epoch = %d, want 9", i, secs[i].Epoch)
		}
	}
	if !reflect.DeepEqual(indexOf(t, body), c.index) {
		t.Fatalf("sink index != object footer index")
	}
	op, err := obs1.DecodeOp(secs[0].Frames[0])
	if err != nil {
		t.Fatal(err)
	}
	ss, ok := op.(obs1.StrSet)
	if !ok || string(ss.Value) != "v" || ss.ExpiryMS != 123 || ss.Ladder != obs1.LadderCounter {
		t.Fatalf("hot-path strset decoded to %+v", op)
	}
}

// indexOf re-parses the object and rebuilds the footer index the slow
// way so the sink's copy can be compared against it.
func indexOf(t *testing.T, body []byte) []obs1.WALIndexEntry {
	t.Helper()
	footOff, footLen, err := obs1.ParseTail(body[len(body)-obs1.TailSize:])
	if err != nil {
		t.Fatal(err)
	}
	idx, err := obs1.ParseWALFooter(body[footOff : footOff+uint64(footLen)])
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestFlusherInOrderDelivery(t *testing.T) {
	store := sim.New(sim.Config{
		Seed: 42,
		Latency: sim.LatencyModel{
			Put: sim.Dist{P50: 2 * time.Millisecond, P99: 20 * time.Millisecond},
		},
	})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 3,
		FlushSize: 64, FlushAge: time.Hour, CapBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	val := make([]byte, 100)
	for seq := uint64(1); seq <= n; seq++ {
		if err := fl.AppendOp(1, 1, opFrame(t, 0, seq, "k", obs1.StrSet{Value: val})); err != nil {
			t.Fatalf("append %d: %v", seq, err)
		}
	}
	if err := fl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(snk.ch)
	want := uint64(1)
	frames := uint32(0)
	for c := range snk.ch {
		if c.walSeq != want {
			t.Fatalf("delivery order broke: got WAL %d, want %d", c.walSeq, want)
		}
		want++
		for _, e := range c.index {
			frames += e.NFrames
		}
	}
	if frames != n {
		t.Fatalf("delivered %d frames total, want %d", frames, n)
	}
}

func TestFlusherCapFlagsNotRefuses(t *testing.T) {
	store := sim.New(sim.Config{
		Seed: 7,
		Latency: sim.LatencyModel{
			Put: sim.Dist{P50: 200 * time.Millisecond, P99: 400 * time.Millisecond},
		},
	})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 4,
		FlushSize: 512, FlushAge: time.Hour, CapBytes: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	if fl.Lagged() {
		t.Fatal("fresh flusher reports lag")
	}
	// Push well past the cap against slow PUTs: every append is accepted,
	// because the cap is the parking threshold the shard gate reads, not
	// an admission bound, and the lag flag rises once buffered plus
	// in-flight bytes top it.
	val := make([]byte, 100)
	seq := uint64(0)
	for range 40 {
		seq++
		if err := fl.AppendOp(1, 1, opFrame(t, 0, seq, "k", obs1.StrSet{Value: val})); err != nil {
			t.Fatalf("append %d: %v", seq, err)
		}
	}
	if !fl.Lagged() {
		t.Fatal("lag flag still down with double the cap buffered")
	}
	// The pipeline drains the backlog on its own; the flag drops at a PUT
	// completion and the flush counter shows the progress the stall window
	// checks.
	deadline := time.Now().Add(10 * time.Second)
	for fl.Lagged() {
		if time.Now().After(deadline) {
			t.Fatal("lag flag never cleared after deliveries resumed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fl.FlushCount() == 0 {
		t.Fatal("flush count still zero after the lag cleared")
	}
}

func TestFlusherEpochConflict(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 5,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	if err := fl.AppendOp(1, 1, opFrame(t, 0, 1, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	err = fl.AppendOp(1, 2, opFrame(t, 0, 2, "k", obs1.KeyDel{}))
	if err == nil || !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("epoch bump into an open buffer gave %v", err)
	}
	fl.Barrier()
	waitCall(t, snk)
	if err := fl.AppendOp(1, 2, opFrame(t, 0, 2, "k", obs1.KeyDel{})); err != nil {
		t.Fatalf("epoch bump after drain: %v", err)
	}
}

func TestFlusherSeqMonotonic(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 6,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()
	if err := fl.AppendOp(1, 1, opFrame(t, 0, 5, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []uint64{4, 5} {
		err := fl.AppendOp(1, 1, opFrame(t, 0, bad, "k", obs1.KeyDel{}))
		if err == nil || !strings.Contains(err.Error(), "strictly increasing") {
			t.Fatalf("seq %d after 5 gave %v", bad, err)
		}
	}
	if err := fl.AppendOp(1, 1, opFrame(t, 0, 6, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
}

func TestFlusherCloseDeliversEverything(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 7,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 20; seq++ {
		if err := fl.AppendOp(uint16(seq%3), 1, opFrame(t, 0, seq, "k", obs1.KeyDel{})); err != nil {
			t.Fatal(err)
		}
	}
	if err := fl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := fl.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	close(snk.ch)
	frames := uint32(0)
	for c := range snk.ch {
		for _, e := range c.index {
			frames += e.NFrames
		}
	}
	if frames != 20 {
		t.Fatalf("Close delivered %d frames, want all 20", frames)
	}
	err = fl.AppendOp(1, 1, opFrame(t, 0, 99, "k", obs1.KeyDel{}))
	if !errors.Is(err, obs1.ErrFlusherClosed) {
		t.Fatalf("append after Close gave %v", err)
	}
}

func TestFlusherSinkError(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	snk.err = errors.New("sink says no")
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 8,
		FlushAge: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fl.AppendOp(1, 1, opFrame(t, 0, 1, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	err = fl.Close()
	if err == nil || !strings.Contains(err.Error(), "sink says no") {
		t.Fatalf("Close after sink error gave %v", err)
	}
	if fl.Err() == nil {
		t.Fatal("Err() nil after sink failure")
	}
}
