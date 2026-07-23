// ledger measures the doc 08 section 9 ledger cells on the landed
// engine end to end: a durability-booted server builds the corpus over
// RESP, ballast pressure demotes and folds it to a counting sim bucket,
// and the reads run through the rebuilt keymap, the directory, and the
// cold reader, the plane the serving node uses. Scores
// PRED-OBS1-O2A-LEDGER after the O2a slices landed, the measured
// companion to the typepoint lab's pre-slice model.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/obs1srv/conformance"
	"github.com/tamnd/aki/obs1srv/drivers"
)

type cfg struct {
	strings int
	fields  int
	members int
}

const (
	hashKey = "ledger:hash"
	setKey  = "ledger:set"
)

func strKey(i int) string    { return "ledger:str:" + strconv.Itoa(i) }
func strVal(i int) string    { return fmt.Sprintf("sv-%027d", i) } // 30 B, safely embedded
func hField(i int) string    { return fmt.Sprintf("f%05d", i) }
func hValue(i int) string    { return fmt.Sprintf("v-%05d", i) }
func setMember(i int) string { return fmt.Sprintf("m%05d", i) }

type lab struct {
	bucket *sim.Sim
	b      *drivers.Booted
	srv    *drivers.Server
	rc     *conformance.Conn
	nc     net.Conn
}

func boot(dir string) (*lab, error) {
	bucket := sim.New(sim.Config{})
	l := &lab{bucket: bucket}
	srv, err := drivers.Listen(drivers.Options{
		Addr: "127.0.0.1:0", Shards: 4, ArenaBytes: 16 << 20, SegBytes: 4 << 20,
		VlogDir: dir, ColdDir: dir, ResidentCapBytes: 2 << 20,
		Boot: func(rt *shard.Runtime) error {
			b, err := drivers.BootDurability(context.Background(), drivers.BootConfig{
				Store: bucket, Prefix: "p", Node: 0xED, Incarnation: 1,
				FlushAge: 5 * time.Millisecond, FoldAge: 20 * time.Millisecond,
			}, rt)
			if err != nil {
				return err
			}
			l.b = b
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	l.srv = srv
	go func() { _ = srv.Serve() }()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		_ = srv.Close()
		return nil, err
	}
	l.nc = nc
	l.rc = conformance.NewConn(nc)
	return l, nil
}

func (l *lab) close() {
	_ = l.nc.Close()
	_ = l.srv.Close()
	_ = l.b.Close()
}

func (l *lab) do(args ...string) string {
	_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
	v, err := l.rc.Do(args)
	if err != nil {
		die("command %v: %v", args, err)
	}
	return conformance.Render(v)
}

func (l *lab) build(c cfg) {
	for i := 0; i < c.strings; i++ {
		if got := l.do("SET", strKey(i), strVal(i)); got != "OK" {
			die("SET %d: %s", i, got)
		}
	}
	const batch = 25
	for base := 0; base < c.fields; base += batch {
		args := []string{"HSET", hashKey}
		for i := base; i < base+batch; i++ {
			args = append(args, hField(i), hValue(i))
		}
		if got := l.do(args...); got != strconv.Itoa(batch) {
			die("HSET at %d: %s", base, got)
		}
	}
	for base := 0; base < c.members; base += batch {
		args := []string{"SADD", setKey}
		for i := base; i < base+batch; i++ {
			args = append(args, setMember(i))
		}
		if got := l.do(args...); got != strconv.Itoa(batch) {
			die("SADD at %d: %s", base, got)
		}
	}
}

// pressure drives ballast rounds, EXISTS boundary spins, and fold
// settles until the folder ledger's Places hold every corpus key, the
// conformance suite's coldness proof.
func (l *lab) pressure(c cfg) int {
	want := []string{hashKey, setKey}
	for i := 0; i < c.strings; i++ {
		want = append(want, strKey(i))
	}
	deadline := time.Now().Add(600 * time.Second)
	round := 0
	for {
		folded := map[string]bool{}
		for _, led := range l.b.Folder.Ledger() {
			for _, p := range led.Places {
				folded[string(p.Key)] = true
			}
		}
		missing := 0
		for _, k := range want {
			if !folded[k] {
				missing++
			}
		}
		if missing == 0 {
			return round
		}
		if time.Now().After(deadline) {
			die("pressure: %d of %d corpus keys never folded", missing, len(want))
		}
		l.ballastRaw(round)
		round++
		for i := 0; i < 50; i++ {
			l.do("EXISTS", hashKey)
			l.do("EXISTS", setKey)
		}
		l.settle()
	}
}

// ballastRaw is ballast with the replies drained as raw lines through
// the conformance reader.
func (l *lab) ballastRaw(round int) {
	const keys, batch = 4000, 500
	val := strings.Repeat("b", 48)
	_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
	for base := 0; base < keys; base += batch {
		var sb strings.Builder
		for i := base; i < base+batch; i++ {
			key := "ballast:" + strconv.Itoa(round) + ":" + strconv.Itoa(i)
			fmt.Fprintf(&sb, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		}
		if _, err := l.nc.Write([]byte(sb.String())); err != nil {
			die("ballast write: %v", err)
		}
		for i := 0; i < batch; i++ {
			line, err := l.rc.R.ReadString('\n')
			if err != nil {
				die("ballast read: %v", err)
			}
			if !strings.HasPrefix(line, "+OK") {
				die("ballast reply %q", line)
			}
		}
	}
}

func (l *lab) settle() {
	deadline := time.Now().Add(30 * time.Second)
	for {
		l.b.Folder.Flush()
		fs := l.b.Folder.Stats()
		if fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut {
			return
		}
		if time.Now().After(deadline) {
			die("fold never settled: %+v", fs)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func groupOf(key string) uint16 {
	_, g := drivers.ClusterMapKey([]byte(key))
	return g
}

// fetchString reads one whole record through the cold plane, blocking.
func (l *lab) fetchString(key string) (obs1.ColdRecord, error) {
	g := groupOf(key)
	loc, ok := l.b.Keymaps[g].Lookup(obs1.Fingerprint([]byte(key)))
	if !ok {
		return obs1.ColdRecord{}, fmt.Errorf("keymap has no entry for %q", key)
	}
	ch := make(chan struct{})
	var rec obs1.ColdRecord
	var err error
	l.b.Cold.Fetch(g, []byte(key), loc, func(r obs1.ColdRecord, e error) {
		rec, err = r, e
		close(ch)
	})
	<-ch
	return rec, err
}

// fetchField reads one collection field through the cold plane, blocking.
func (l *lab) fetchField(collKey, field string) (obs1.ColdField, error) {
	g := groupOf(collKey)
	loc, ok := l.b.Keymaps[g].Lookup(obs1.Fingerprint([]byte(collKey)))
	if !ok {
		return obs1.ColdField{}, fmt.Errorf("keymap has no entry for %q", collKey)
	}
	ch := make(chan struct{})
	var f obs1.ColdField
	var err error
	l.b.Cold.FetchField(g, []byte(collKey), loc, []byte(field), time.Now().UnixMilli(), func(r obs1.ColdField, e error) {
		f, err = r, e
		close(ch)
	})
	<-ch
	return f, err
}

type cell struct {
	name  string
	ops   int
	gets  int64
	bytes int64
	found int
}

func (c cell) row() string {
	perOp := 0.0
	bytesOp := 0.0
	foundPct := 0.0
	if c.ops > 0 {
		perOp = float64(c.gets) / float64(c.ops)
		bytesOp = float64(c.bytes) / float64(c.ops)
		foundPct = 100 * float64(c.found) / float64(c.ops)
	}
	return fmt.Sprintf("%s,%d,%d,%.4f,%.1f,%.1f", c.name, c.ops, c.gets, perOp, bytesOp/1024, foundPct)
}

// die aborts the run; run's recover turns it into an error so the smoke
// test fails cleanly instead of exiting the process.
func die(format string, args ...any) {
	panic(fmt.Errorf("ledger: "+format, args...))
}

type results struct {
	rounds  int
	cells   []cell
	kmKeys  int
	kmBytes int
	coll    int
	share   float64
	cold    obs1.ColdReadStats
}

func main() {
	quick := flag.Bool("quick", false, "small corpus for a fast pass")
	flag.Parse()
	c := cfg{strings: 2000, fields: 20000, members: 20000}
	if *quick {
		c = cfg{strings: 200, fields: 1500, members: 600}
	}
	res, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("# folded after %d pressure rounds\n", res.rounds)
	fmt.Println("cell,ops,gets,gets_per_op,kib_per_op,found_pct")
	for _, row := range res.cells {
		fmt.Println(row.row())
	}
	fmt.Printf("keymap_bytes_per_key,%d,%d,%.2f,,\n", res.kmKeys, res.kmBytes, float64(res.kmBytes)/float64(res.kmKeys))
	fmt.Printf("dir_coll_share_b_per_elem,%d,%d,%.4f,,\n", c.fields+c.members, res.coll, res.share)
	fmt.Printf("# cold stats: %+v\n", res.cold)
}

func run(c cfg) (res results, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				return
			}
			panic(r)
		}
	}()

	dir, derr := os.MkdirTemp("", "obs1-ledger-*")
	if derr != nil {
		return res, derr
	}
	defer func() { _ = os.RemoveAll(dir) }()
	l, berr := boot(dir)
	if berr != nil {
		return res, berr
	}
	defer l.close()

	l.build(c)
	res.rounds = l.pressure(c)

	// Every string, serially, through the cold plane.
	u0 := l.bucket.Usage()
	sc := cell{name: "string_get", ops: c.strings}
	for i := 0; i < c.strings; i++ {
		rec, ferr := l.fetchString(strKey(i))
		if ferr != nil {
			die("string %d: %v", i, ferr)
		}
		if rec.Found && string(rec.Value) == strVal(i) {
			sc.found++
		}
	}
	u1 := l.bucket.Usage()
	sc.gets, sc.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, sc)

	// The string miss: an absent fingerprint answers at the keymap.
	u0 = l.bucket.Usage()
	miss := cell{name: "string_miss", ops: 100}
	for i := 0; i < 100; i++ {
		key := "ledger:absent:" + strconv.Itoa(i)
		g := groupOf(key)
		if _, ok := l.b.Keymaps[g].Lookup(obs1.Fingerprint([]byte(key))); !ok {
			miss.found++
		}
	}
	u1 = l.bucket.Usage()
	miss.gets, miss.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, miss)

	// Every hash field, serially.
	u0 = l.bucket.Usage()
	hc := cell{name: "hget", ops: c.fields}
	for i := 0; i < c.fields; i++ {
		f, ferr := l.fetchField(hashKey, hField(i))
		if ferr != nil {
			die("hget %d: %v", i, ferr)
		}
		if f.Found && string(f.Value) == hValue(i) {
			hc.found++
		}
	}
	u1 = l.bucket.Usage()
	hc.gets, hc.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, hc)

	// The field miss: the floor picks a chunk, the chunk answers absent.
	u0 = l.bucket.Usage()
	fm := cell{name: "hget_miss", ops: 100}
	for i := 0; i < 100; i++ {
		f, ferr := l.fetchField(hashKey, "zz-absent-"+strconv.Itoa(i))
		if ferr != nil {
			die("hget miss %d: %v", i, ferr)
		}
		if !f.Found {
			fm.found++
		}
	}
	u1 = l.bucket.Usage()
	fm.gets, fm.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, fm)

	// Every set member, serially; a set chunk is a valueless hash chunk.
	u0 = l.bucket.Usage()
	mc := cell{name: "sismember", ops: c.members}
	for i := 0; i < c.members; i++ {
		f, ferr := l.fetchField(setKey, setMember(i))
		if ferr != nil {
			die("sismember %d: %v", i, ferr)
		}
		if f.Found && len(f.Value) == 0 {
			mc.found++
		}
	}
	u1 = l.bucket.Usage()
	mc.gets, mc.bytes = u1.GetRequests-u0.GetRequests, u1.BytesDown-u0.BytesDown
	res.cells = append(res.cells, mc)

	// Resident cells, measured off the live index structures.
	for _, km := range l.b.Keymaps {
		res.kmBytes += km.Bytes()
		res.kmKeys += km.Len()
	}
	dirBytes, dirChunks := 0, 0
	for _, d := range l.b.Dirs {
		dirBytes += d.Bytes()
		dirChunks += d.Chunks()
	}
	for _, key := range []string{hashKey, setKey} {
		g := groupOf(key)
		loc, ok := l.b.Keymaps[g].Lookup(obs1.Fingerprint([]byte(key)))
		if !ok {
			die("keymap lost %q", key)
		}
		res.coll += len(l.b.Dirs[g].CollChunks(loc, obs1.Fingerprint([]byte(key))))
	}
	res.share = float64(dirBytes) / float64(dirChunks) * float64(res.coll) / float64(c.fields+c.members)

	res.cold = l.b.Cold.Stats()
	if res.cold.Errs != 0 || res.cold.Unresolved != 0 {
		die("cold reader errs %d unresolved %d", res.cold.Errs, res.cold.Unresolved)
	}
	return res, nil
}
