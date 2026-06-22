package command

import (
	"strings"
	"testing"
)

// aclLineFor returns the ACL LIST line for a user, or "" if absent.
func aclLineFor(d *Dispatcher, name string) string {
	for _, n := range d.acl.usernames() {
		if n == name {
			return aclLine(d.acl.get(name))
		}
	}
	return ""
}

// TestACLPubsubDefaultResetchannels checks a new user starts with no channel
// access under the default policy.
func TestACLPubsubDefaultResetchannels(t *testing.T) {
	d := newMetricsDispatcher(t)
	if got := runReply(d, "ACL", "SETUSER", "alice", "on", ">pw", "~*", "+@all"); got != "+OK\r\n" {
		t.Fatalf("ACL SETUSER alice = %q", got)
	}

	line := aclLineFor(d, "alice")
	if strings.Contains(line, "&") {
		t.Fatalf("alice got channel access under resetchannels default: %q", line)
	}
}

// TestACLPubsubDefaultAllchannels checks a new user created after setting
// allchannels starts with a &* rule, while the default policy stays restrictive.
func TestACLPubsubDefaultAllchannels(t *testing.T) {
	d := newMetricsDispatcher(t)
	if got := runReply(d, "CONFIG", "SET", "acl-pubsub-default", "allchannels"); got != "+OK\r\n" {
		t.Fatalf("CONFIG SET acl-pubsub-default = %q", got)
	}

	if got := runReply(d, "ACL", "SETUSER", "bob", "on", ">pw", "~*", "+@all"); got != "+OK\r\n" {
		t.Fatalf("ACL SETUSER bob = %q", got)
	}
	line := aclLineFor(d, "bob")
	if !strings.Contains(line, "&*") {
		t.Fatalf("bob has no allchannels rule under allchannels default: %q", line)
	}

	// Back to the restrictive default and a fresh user starts closed again.
	if got := runReply(d, "CONFIG", "SET", "acl-pubsub-default", "resetchannels"); got != "+OK\r\n" {
		t.Fatalf("CONFIG SET acl-pubsub-default resetchannels = %q", got)
	}
	if got := runReply(d, "ACL", "SETUSER", "carol", "on", ">pw", "~*", "+@all"); got != "+OK\r\n" {
		t.Fatalf("ACL SETUSER carol = %q", got)
	}
	if line := aclLineFor(d, "carol"); strings.Contains(line, "&") {
		t.Fatalf("carol got channel access after switching back to resetchannels: %q", line)
	}
}

// TestACLPubsubDefaultExistingUser checks the default only seeds brand new users:
// editing an existing user does not silently grant channels.
func TestACLPubsubDefaultExistingUser(t *testing.T) {
	d := newMetricsDispatcher(t)
	if got := runReply(d, "ACL", "SETUSER", "dave", "on", ">pw", "~*", "+@all"); got != "+OK\r\n" {
		t.Fatalf("ACL SETUSER dave = %q", got)
	}
	if got := runReply(d, "CONFIG", "SET", "acl-pubsub-default", "allchannels"); got != "+OK\r\n" {
		t.Fatalf("CONFIG SET acl-pubsub-default = %q", got)
	}
	// dave already exists, so a later edit does not pick up the new default.
	if got := runReply(d, "ACL", "SETUSER", "dave", "+get"); got != "+OK\r\n" {
		t.Fatalf("ACL SETUSER dave +get = %q", got)
	}
	if line := aclLineFor(d, "dave"); strings.Contains(line, "&") {
		t.Fatalf("existing user dave gained channels from the new default: %q", line)
	}
}
