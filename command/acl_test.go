package command

import (
	"strings"
	"testing"
)

func TestACLWhoamiDefault(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "ACL WHOAMI"); got != "default" {
		t.Fatalf("ACL WHOAMI = %q", got)
	}
}

func TestACLUsersAndList(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "ACL SETUSER alice on >pw ~foo:* +get"); got != "+OK" {
		t.Fatalf("SETUSER = %q", got)
	}
	users := bulkSlice(t, sendReply(t, r, c, "ACL USERS"))
	if !hasStr(users, "default") || !hasStr(users, "alice") {
		t.Fatalf("ACL USERS = %v", users)
	}
	lines := bulkSlice(t, sendReply(t, r, c, "ACL LIST"))
	var aliceLine string
	for _, l := range lines {
		if strings.HasPrefix(l, "user alice ") {
			aliceLine = l
		}
	}
	if aliceLine == "" {
		t.Fatalf("alice not in ACL LIST: %v", lines)
	}
	if !strings.Contains(aliceLine, " on") || !strings.Contains(aliceLine, "~foo:*") || !strings.Contains(aliceLine, "+get") {
		t.Fatalf("alice line = %q", aliceLine)
	}
}

func TestACLGetUser(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER bob on nopass ~* +@all")
	m := asArray(t, sendReply(t, r, c, "ACL GETUSER bob"))
	fields := map[string]any{}
	for i := 0; i+1 < len(m); i += 2 {
		fields[m[i].(string)] = m[i+1]
	}
	flags := bulkSlice(t, fields["flags"])
	if !hasStr(flags, "on") || !hasStr(flags, "nopass") || !hasStr(flags, "allkeys") {
		t.Fatalf("bob flags = %v", flags)
	}
	if fields["commands"] != "+@all" {
		t.Fatalf("bob commands = %v", fields["commands"])
	}
}

func TestACLGetUserMissing(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "ACL GETUSER ghost"); got != "$-1" && got != "_" {
		t.Fatalf("ACL GETUSER ghost = %q", got)
	}
}

func TestACLDelUser(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER carol on nopass")
	if got := sendLine(t, r, c, "ACL DELUSER carol"); got != ":1" {
		t.Fatalf("DELUSER carol = %q", got)
	}
	if got := sendLine(t, r, c, "ACL DELUSER default"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("DELUSER default = %q", got)
	}
}

func TestACLCat(t *testing.T) {
	r, c := startData(t)
	cats := bulkSlice(t, sendReply(t, r, c, "ACL CAT"))
	if !hasStr(cats, "read") || !hasStr(cats, "write") || !hasStr(cats, "string") {
		t.Fatalf("ACL CAT = %v", cats)
	}
	cmds := bulkSlice(t, sendReply(t, r, c, "ACL CAT string"))
	if !hasStr(cmds, "get") || !hasStr(cmds, "set") {
		t.Fatalf("ACL CAT string = %v", cmds)
	}
	if got := sendLine(t, r, c, "ACL CAT nosuchcat"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("ACL CAT nosuchcat = %q", got)
	}
}

func TestACLGenPass(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "ACL GENPASS"); len(got) != 64 {
		t.Fatalf("ACL GENPASS len = %d", len(got))
	}
	if got := bulk(t, r, c, "ACL GENPASS 32"); len(got) != 8 {
		t.Fatalf("ACL GENPASS 32 len = %d", len(got))
	}
	if got := sendLine(t, r, c, "ACL GENPASS 9999"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("ACL GENPASS 9999 = %q", got)
	}
}

func TestACLEnforceCommand(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER reader on >pw ~* +@read +select +auth")
	if got := sendLine(t, r, c, "AUTH reader pw"); got != "+OK" {
		t.Fatalf("AUTH reader = %q", got)
	}
	if got := sendLine(t, r, c, "GET foo"); got != "$-1" && got != "_" {
		t.Fatalf("reader GET = %q", got)
	}
	got := sendLine(t, r, c, "SET foo bar")
	if !strings.HasPrefix(got, "-NOPERM") || !strings.Contains(got, "'set'") {
		t.Fatalf("reader SET = %q", got)
	}
}

func TestACLEnforceKey(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER keyed on >pw ~app:* +@all")
	sendLine(t, r, c, "AUTH keyed pw")
	if got := sendLine(t, r, c, "SET app:1 v"); got != "+OK" {
		t.Fatalf("SET app:1 = %q", got)
	}
	got := sendLine(t, r, c, "SET other v")
	if !strings.HasPrefix(got, "-NOPERM") || !strings.Contains(got, "access a key") {
		t.Fatalf("SET other = %q", got)
	}
}

func TestACLEnforceChannel(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER pub on >pw ~* +@all &news:*")
	sendLine(t, r, c, "AUTH pub pw")
	if got := sendLine(t, r, c, "PUBLISH news:sport hi"); got != ":0" {
		t.Fatalf("PUBLISH news:sport = %q", got)
	}
	got := sendLine(t, r, c, "PUBLISH other hi")
	if !strings.HasPrefix(got, "-NOPERM") || !strings.Contains(got, "access a channel") {
		t.Fatalf("PUBLISH other = %q", got)
	}
}

func TestACLAuthDisabledUser(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER off1 off >pw ~* +@all")
	if got := sendLine(t, r, c, "AUTH off1 pw"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("AUTH off1 = %q", got)
	}
}

func TestACLDryRun(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER dr on >pw ~foo:* +get")
	if got := sendLine(t, r, c, "ACL DRYRUN dr get foo:1"); got != "+OK" {
		t.Fatalf("DRYRUN allowed = %q", got)
	}
	got := sendLine(t, r, c, "ACL DRYRUN dr set foo:1 v")
	if !strings.HasPrefix(got, "-NOPERM") {
		t.Fatalf("DRYRUN set = %q", got)
	}
	got = sendLine(t, r, c, "ACL DRYRUN dr get bar:1")
	if !strings.HasPrefix(got, "-NOPERM") || !strings.Contains(got, "access a key") {
		t.Fatalf("DRYRUN key = %q", got)
	}
	if got := sendLine(t, r, c, "ACL DRYRUN ghost get x"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("DRYRUN ghost = %q", got)
	}
}

func TestACLLog(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER lg on >pw ~* +@read +select +auth +acl")
	sendLine(t, r, c, "AUTH lg pw")
	sendLine(t, r, c, "SET k v") // denied, logged
	entries := asArray(t, sendReply(t, r, c, "ACL LOG"))
	if len(entries) == 0 {
		t.Fatalf("ACL LOG empty after a denial")
	}
	first := asArray(t, entries[0])
	fields := map[string]any{}
	for i := 0; i+1 < len(first); i += 2 {
		fields[first[i].(string)] = first[i+1]
	}
	if fields["reason"] != "cmd" || fields["object"] != "set" {
		t.Fatalf("log entry = %v", fields)
	}
	if got := sendLine(t, r, c, "ACL LOG RESET"); got != "+OK" {
		t.Fatalf("ACL LOG RESET = %q", got)
	}
}

func TestACLLogMaxLen(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER lm on >pw ~* +@read +select +auth +acl +config")
	sendLine(t, r, c, "AUTH lm pw")

	// Four distinct denials, different objects so they do not coalesce.
	for _, cmd := range []string{"SET k v", "LPUSH l v", "HSET h f v", "SADD s m"} {
		if got := sendLine(t, r, c, cmd); !strings.HasPrefix(got, "-NOPERM") {
			t.Fatalf("%s = %q want NOPERM", cmd, got)
		}
	}
	if n := len(asArray(t, sendReply(t, r, c, "ACL LOG"))); n != 4 {
		t.Fatalf("ACL LOG has %d entries want 4", n)
	}

	// Shrinking the cap trims the log right away.
	if got := sendLine(t, r, c, "CONFIG SET acllog-max-len 2"); got != "+OK" {
		t.Fatalf("CONFIG SET acllog-max-len = %q", got)
	}
	if n := len(asArray(t, sendReply(t, r, c, "ACL LOG"))); n != 2 {
		t.Fatalf("after shrink ACL LOG has %d entries want 2", n)
	}

	// New denials still respect the cap.
	for _, cmd := range []string{"SET k2 v", "LPUSH l2 v", "HSET h2 f v"} {
		sendLine(t, r, c, cmd)
	}
	if n := len(asArray(t, sendReply(t, r, c, "ACL LOG"))); n != 2 {
		t.Fatalf("ACL LOG grew past cap to %d want 2", n)
	}
}

func TestACLAuthThenWhoami(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "ACL SETUSER who on >pw ~* +@all")
	sendLine(t, r, c, "AUTH who pw")
	if got := bulk(t, r, c, "ACL WHOAMI"); got != "who" {
		t.Fatalf("WHOAMI after auth = %q", got)
	}
}

func TestACLFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/users.acl"
	a := newACLRegistry("")
	u := &aclUser{name: "svc"}
	if err := applyACLRules(u, []string{"on", "#" + hashPassword("pw"), "~svc:*", "&svc.*", "+get", "+set"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	a.users["svc"] = u
	a.aclFile = path
	if err := a.saveFile(); err != nil {
		t.Fatalf("save: %v", err)
	}

	b := newACLRegistry("")
	b.aclFile = path
	if err := b.loadFile(); err != nil {
		t.Fatalf("load: %v", err)
	}
	got := b.get("svc")
	if got == nil || !got.on {
		t.Fatalf("svc not restored: %v", got)
	}
	if !got.checkPassword("pw") {
		t.Fatalf("svc password not restored")
	}
	if len(got.keyRules) != 1 || got.keyRules[0].pattern != "svc:*" {
		t.Fatalf("svc keys = %v", got.keyRules)
	}
	if b.get("default") == nil {
		t.Fatalf("default user missing after load")
	}
}
