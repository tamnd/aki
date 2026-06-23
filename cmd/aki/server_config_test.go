package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aki.conf")
	body := "" +
		"# a comment\n" +
		"\n" +
		"port 7011\n" +
		"  bind 127.0.0.1 -::1\n" +
		"appendfsync always\n" +
		"save \"3600 1 300 100\"\n" +
		"requirepass \"\"\n" +
		"DIR /var/lib/aki\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	conf, err := parseConfigFile(path)
	if err != nil {
		t.Fatalf("parseConfigFile: %v", err)
	}
	want := map[string]string{
		"port":        "7011",
		"bind":        "127.0.0.1 -::1",
		"appendfsync": "always",
		"save":        "3600 1 300 100",
		"requirepass": "",
		"dir":         "/var/lib/aki", // directive name lowercased
	}
	for k, v := range want {
		if got, ok := conf[k]; !ok || got != v {
			t.Errorf("conf[%q] = %q (present=%v), want %q", k, got, ok, v)
		}
	}
	if len(conf) != len(want) {
		t.Errorf("got %d directives, want %d: %v", len(conf), len(want), conf)
	}
}

func TestParseConfigFileMissing(t *testing.T) {
	if _, err := parseConfigFile(filepath.Join(t.TempDir(), "nope.conf")); err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestDequote(t *testing.T) {
	cases := map[string]string{
		`"3600 1"`: "3600 1",
		`""`:       "",
		`plain`:    "plain",
		`"`:        `"`, // a lone quote is not a matched pair
		`"unterminated`: `"unterminated`,
	}
	for in, want := range cases {
		if got := dequote(in); got != want {
			t.Errorf("dequote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFirstField(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1 -::1": "127.0.0.1",
		"localhost":      "localhost",
		"a\tb":           "a",
		"":               "",
	}
	for in, want := range cases {
		if got := firstField(in); got != want {
			t.Errorf("firstField(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseIntFlag(t *testing.T) {
	if n, ok := parseIntFlag("16"); !ok || n != 16 {
		t.Errorf("parseIntFlag(16) = %d,%v", n, ok)
	}
	if _, ok := parseIntFlag(""); ok {
		t.Error("parseIntFlag(empty) should fail")
	}
	if _, ok := parseIntFlag("12x"); ok {
		t.Error("parseIntFlag(12x) should fail")
	}
}

func TestResolveSave(t *testing.T) {
	// Explicit --save "" disables snapshots even though the config file sets it.
	got := resolveSave(map[string]bool{"save": true}, map[string]string{"save": "3600 1"}, "")
	if got != "" {
		t.Errorf("explicit empty --save should win, got %q", got)
	}
	// Config-file value is used when the flag is absent.
	got = resolveSave(map[string]bool{}, map[string]string{"save": "3600 1"}, "")
	if got != "3600 1" {
		t.Errorf("config-file save should be used, got %q", got)
	}
	// Flag value wins over file when both are present.
	got = resolveSave(map[string]bool{"save": true}, map[string]string{"save": "3600 1"}, "900 1")
	if got != "900 1" {
		t.Errorf("flag save should win, got %q", got)
	}
}
