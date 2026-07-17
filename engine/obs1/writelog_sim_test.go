package obs1_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// The composed pipeline is the runtime's durability seam.
var _ shard.WriteLog = (*obs1.WriteLog)(nil)

// testMapKey routes a key by its first letter, four groups, so a test
// controls group assignment without the cluster hash: "alpha" lands on
// group 0, "bravo" on group 1.
func testMapKey(key []byte) (uint16, uint16) {
	return uint16(key[0]), uint16(key[0]-'a') % 4
}

// logRig is the slice 4 wiring below the write log: a chain appender
// whose fold the write log claims. Unlike commitRig it leaves the
// fold's OnCommit hook free, because claiming it is NewWriteLog's job.
type logRig struct {
	store *sim.Sim
	fold  *obs1.LeaseFold
	ap    *obs1.ChainAppender
}

func newLogRig(t *testing.T, store *sim.Sim, node uint64) *logRig {
	t.Helper()
	fold := obs1.NewLeaseFold()
	ap, err := obs1.NewChainAppender(store, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	return &logRig{store: store, fold: fold, ap: ap}
}

func (r *logRig) grant(t *testing.T, node uint64, epoch uint32, groups ...uint16) {
	t.Helper()
	recs := make([]obs1.ChainRecord, 0, len(groups))
	for _, g := range groups {
		recs = append(recs, obs1.GrantRecord{Group: g, Node: node, Epoch: epoch})
	}
	if _, err := r.ap.Append(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
}

func newTestLog(t *testing.T, rig *logRig, node uint64, cfg obs1.WriteLogConfig) *obs1.WriteLog {
	t.Helper()
	cfg.Store = rig.store
	cfg.Prefix = "p"
	cfg.Node = node
	cfg.Chain = rig.ap
	cfg.Fold = rig.fold
	if cfg.Groups == 0 {
		cfg.Groups = 4
	}
	if cfg.MapKey == nil {
		cfg.MapKey = testMapKey
	}
	if cfg.FlushAge == 0 {
		cfg.FlushAge = time.Hour
	}
	wl, err := obs1.NewWriteLog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return wl
}

// durabilityRows parses the AppendInfo section into a counter map.
func durabilityRows(t *testing.T, wl *obs1.WriteLog) map[string]uint64 {
	t.Helper()
	rows := make(map[string]uint64)
	for _, line := range strings.Split(string(wl.AppendInfo(nil)), "\r\n") {
		name, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			t.Fatalf("row %q: %v", line, err)
		}
		rows[name] = n
	}
	return rows
}

// walObject fetches one flushed WAL object and parses its sections.
func walObject(t *testing.T, store *sim.Sim, node, walSeq uint64) []obs1.WALSection {
	t.Helper()
	key := fmt.Sprintf("p/wal/%016x/%016d", node, walSeq)
	body, _, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	secs, _, err := obs1.ParseWAL(body)
	if err != nil {
		t.Fatalf("ParseWAL: %v", err)
	}
	return secs
}

func TestWriteLogEndToEnd(t *testing.T) {
	const node = uint64(0xD1)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0, 1)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})
	wl.SetGroup(0, 1, 1)
	wl.SetGroup(1, 1, 1)

	// Three frames on group 0, one on group 1: the seq streams are
	// per group, allocated by the emissions themselves.
	if err := wl.StrSet([]byte("alpha"), []byte("v1"), 0, false); err != nil {
		t.Fatal(err)
	}
	if err := wl.StrSet([]byte("alpha"), []byte("7"), 12345, true); err != nil {
		t.Fatal(err)
	}
	if err := wl.KeyDel([]byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if err := wl.StrSet([]byte("bravo"), []byte("v2"), 0, false); err != nil {
		t.Fatal(err)
	}
	wl.Barrier()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 3); err != nil {
		t.Fatalf("Wait group 0: %v", err)
	}
	if err := wl.Marks().Wait(ctx, 1, 1); err != nil {
		t.Fatalf("Wait group 1: %v", err)
	}

	secs := walObject(t, store, node, 1)
	if len(secs) != 2 || secs[0].Group != 0 || secs[1].Group != 1 {
		t.Fatalf("sections = %+v, want groups 0 and 1", secs)
	}
	if len(secs[0].Frames) != 3 || len(secs[1].Frames) != 1 {
		t.Fatalf("frame counts = %d and %d, want 3 and 1", len(secs[0].Frames), len(secs[1].Frames))
	}
	for i, f := range secs[0].Frames {
		if f.Seq != uint64(i+1) {
			t.Fatalf("group 0 frame %d seq = %d", i, f.Seq)
		}
	}
	op0, err := obs1.DecodeOp(secs[0].Frames[0])
	if err != nil {
		t.Fatal(err)
	}
	if ss := op0.(obs1.StrSet); string(ss.Value) != "v1" || ss.ExpiryMS != 0 || ss.Ladder != 0 {
		t.Fatalf("frame 1 = %+v", ss)
	}
	op1, err := obs1.DecodeOp(secs[0].Frames[1])
	if err != nil {
		t.Fatal(err)
	}
	if ss := op1.(obs1.StrSet); string(ss.Value) != "7" || ss.ExpiryMS != 12345 || ss.Ladder != obs1.LadderCounter {
		t.Fatalf("counter frame = %+v", ss)
	}
	op2, err := obs1.DecodeOp(secs[0].Frames[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := op2.(obs1.KeyDel); !ok {
		t.Fatalf("frame 3 = %+v, want a keydel", op2)
	}
	opB, err := obs1.DecodeOp(secs[1].Frames[0])
	if err != nil {
		t.Fatal(err)
	}
	if ss := opB.(obs1.StrSet); string(ss.Value) != "v2" || secs[1].Frames[0].Seq != 1 {
		t.Fatalf("group 1 frame = %+v seq %d", ss, secs[1].Frames[0].Seq)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_barrier_flushes"] != 1 || rows["chain_commit_records"] != 1 {
		t.Fatalf("rows = %v, want one barrier flush committing one record", rows)
	}
	for _, name := range []string{"wal_encode_errors", "wal_stall_errors", "wal_fatal_errors", "wal_epoch_errors"} {
		if rows[name] != 0 {
			t.Fatalf("%s = %d on the clean path", name, rows[name])
		}
	}
	if err := wl.Err(); err != nil {
		t.Fatal(err)
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteLogConfigErrors(t *testing.T) {
	const node = uint64(0xD2)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	base := func() obs1.WriteLogConfig {
		return obs1.WriteLogConfig{
			Store: store, Prefix: "p", Node: node, Chain: rig.ap,
			Fold: rig.fold, Groups: 4, MapKey: testMapKey,
		}
	}
	cfg := base()
	cfg.Fold = nil
	if _, err := obs1.NewWriteLog(cfg); err == nil || !strings.Contains(err.Error(), "lease fold") {
		t.Fatalf("nil fold gave %v", err)
	}
	cfg = base()
	cfg.Groups = 0
	if _, err := obs1.NewWriteLog(cfg); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("zero groups gave %v", err)
	}
	cfg = base()
	cfg.MapKey = nil
	if _, err := obs1.NewWriteLog(cfg); err == nil || !strings.Contains(err.Error(), "key mapper") {
		t.Fatalf("nil mapper gave %v", err)
	}
	claimed := obs1.NewLeaseFold()
	claimed.OnCommit = obs1.NewWatermarks().ApplyVerdict
	cfg = base()
	cfg.Fold = claimed
	if _, err := obs1.NewWriteLog(cfg); err == nil || !strings.Contains(err.Error(), "already claimed") {
		t.Fatalf("claimed fold gave %v", err)
	}
	// A failed construction leaves the fold unclaimed, so the caller
	// can fix the config and retry against the same fold.
	cfg = base()
	cfg.Store = nil
	if _, err := obs1.NewWriteLog(cfg); err == nil {
		t.Fatal("nil store built a write log")
	}
	if rig.fold.OnCommit != nil {
		t.Fatal("failed construction left the fold's OnCommit claimed")
	}
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteLogEpochMissing(t *testing.T) {
	const node = uint64(0xD3)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})

	// No SetGroup: dispatch should never route here, so the emission
	// fails the command without touching the flusher.
	if err := wl.StrSet([]byte("alpha"), []byte("v"), 0, false); err == nil || err.Error() != "ERR internal: wal epoch" {
		t.Fatalf("StrSet before SetGroup gave %v", err)
	}
	if err := wl.KeyDel([]byte("alpha")); err == nil || err.Error() != "ERR internal: wal epoch" {
		t.Fatalf("KeyDel before SetGroup gave %v", err)
	}
	rows := durabilityRows(t, wl)
	if rows["wal_epoch_errors"] != 2 || rows["wal_flushes"] != 0 {
		t.Fatalf("rows = %v, want 2 epoch errors and no flushes", rows)
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteLogStallBurnsNoSeq(t *testing.T) {
	const node = uint64(0xD4)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{
		FlushSize: 1 << 20, CapBytes: 200,
	})
	wl.SetGroup(0, 1, 1)

	// A frame past the cap fails with the stall text (parking arrives
	// with slice 7) and must not consume the group's next seq.
	big := strings.Repeat("x", 256)
	if err := wl.StrSet([]byte("alpha"), []byte(big), 0, false); err == nil || err.Error() != "ERR store: flush stalled" {
		t.Fatalf("over-cap StrSet gave %v", err)
	}
	if err := wl.StrSet([]byte("alpha"), []byte("v"), 0, false); err != nil {
		t.Fatal(err)
	}
	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 1); err != nil {
		t.Fatal(err)
	}
	secs := walObject(t, store, node, 1)
	if len(secs) != 1 || len(secs[0].Frames) != 1 || secs[0].Frames[0].Seq != 1 {
		t.Fatalf("sections = %+v, want the surviving frame at seq 1", secs)
	}
	rows := durabilityRows(t, wl)
	if rows["wal_stall_errors"] != 1 {
		t.Fatalf("wal_stall_errors = %d, want 1", rows["wal_stall_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteLogClosedIsFatal(t *testing.T) {
	const node = uint64(0xD5)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})
	wl.SetGroup(0, 1, 1)
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wl.StrSet([]byte("alpha"), []byte("v"), 0, false); err == nil || err.Error() != "ERR store: flush stalled" {
		t.Fatalf("StrSet after Close gave %v", err)
	}
	if rows := durabilityRows(t, wl); rows["wal_fatal_errors"] != 1 {
		t.Fatalf("wal_fatal_errors = %d, want 1", rows["wal_fatal_errors"])
	}
}
