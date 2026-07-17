package main

import (
	"bytes"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestParseBenchLine(t *testing.T) {
	r, ok := parseBenchLine("-1      1048672 (1.000) 13897.14 MB/s 70571.9 MB/s  zb.bin")
	if !ok || r.level != 1 || r.compBytes != 1048672 || r.compMBps != 13897.14 || r.decMBps != 70571.9 {
		t.Fatalf("row = %+v ok = %v", r, ok)
	}
	for _, bad := range []string{
		"bench 1.5.7 : input 1048576 bytes, 1 seconds, 128 KB blocks",
		"",
		"-x 12 (1.0) 1 MB/s 2 MB/s f",
	} {
		if _, ok := parseBenchLine(bad); ok {
			t.Fatalf("parsed %q", bad)
		}
	}
}

func TestCorpusDeterministic(t *testing.T) {
	a := concat(buildCorpus("j", 1<<20, 1, jsonSess))
	b := concat(buildCorpus("j", 1<<20, 1, jsonSess))
	if !bytes.Equal(a, b) {
		t.Fatal("same seed produced different corpora")
	}
	c := concat(buildCorpus("j", 1<<20, 2, jsonSess))
	if bytes.Equal(a, c) {
		t.Fatal("different seeds produced identical corpora")
	}
}

func TestWalStreamArith(t *testing.T) {
	c := buildCorpus("n", 1<<18, 7, numSer)
	want := c.size + len(c.vals)*(frameHdrBytes+frameKeyBytes)
	if got := len(walStream(c)); got != want {
		t.Fatalf("wal stream = %d bytes, want %d", got, want)
	}
}

func TestMixedShares(t *testing.T) {
	c := buildMixed(8<<20, 5)
	var shares [4]int
	for _, v := range c.vals {
		switch {
		case len(v) > 0 && v[0] == '{':
			shares[0] += len(v)
		case len(v) == 2048:
			shares[1] += len(v)
		case len(v) == 200:
			shares[3] += len(v)
		default:
			shares[2] += len(v)
		}
	}
	want := [4]int{600, 200, 100, 100}
	for i, s := range shares {
		got := s * 1000 / c.size
		if got < want[i]-20 || got > want[i]+20 {
			t.Fatalf("family %d holds %d per mille of bytes, want about %d", i, got, want[i])
		}
	}
}

func TestDerivedMath(t *testing.T) {
	if got := decodeUsPerUnit(131072, 1310.72); math.Abs(got-100) > 1e-9 {
		t.Fatalf("decode us = %v, want 100", got)
	}
	if got := cpuSecPerGiB(1073.741824); math.Abs(got-1) > 1e-9 {
		t.Fatalf("cpu sec per GiB = %v, want 1", got)
	}
}

func TestStorageDollar(t *testing.T) {
	if got := storageUSDPerRawGBMonth(1.0); math.Abs(got-0.023) > 1e-6 {
		t.Fatalf("full storage = %v, want 0.023", got)
	}
	if got := storageUSDPerRawGBMonth(0.5); math.Abs(got-0.0115) > 1e-6 {
		t.Fatalf("half storage = %v, want 0.0115", got)
	}
}

// TestZstdRoundTrip drives the real binary when one is present; the
// scored sweep depends on it, CI does not.
func TestZstdRoundTrip(t *testing.T) {
	bin, err := exec.LookPath("zstd")
	if err != nil {
		t.Skip("no zstd binary on PATH")
	}
	raw := concat(buildCorpus("j", 1<<18, 9, jsonSess))
	dir := t.TempDir()
	path := filepath.Join(dir, "rt.bin")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bin, "-1", "-q", path, "-o", path+".zst").CombinedOutput(); err != nil {
		t.Fatalf("compress: %v\n%s", err, out)
	}
	if out, err := exec.Command(bin, "-d", "-q", path+".zst", "-o", path+".rt").CombinedOutput(); err != nil {
		t.Fatalf("decompress: %v\n%s", err, out)
	}
	back, err := os.ReadFile(path + ".rt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, back) {
		t.Fatal("round trip changed the bytes")
	}
}
