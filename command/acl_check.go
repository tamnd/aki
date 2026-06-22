package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tamnd/aki/networking"
)

// This file holds the ACL checks the dispatch pipeline runs before a command
// executes, the channel extraction the Pub/Sub check needs, the denial log, and
// the aclfile load and save (spec 2064 doc 19 sections 13, 12.9 and 12.11).

// aclEnforce runs the full ACL check for a command about to execute. It returns
// the error string to send the client, or "" when the command is allowed. It
// logs any denial against the connection.
func (d *Dispatcher) aclEnforce(c *networking.Conn, sess *session, cmd *CmdDesc, argv [][]byte) string {
	// An unauthenticated connection may only run no-auth commands.
	if !sess.authenticated {
		if cmd.Flags.Has(FlagNoAuth) {
			return ""
		}
		d.acl.addLog(c, sess, "auth", "toplevel", cmd.Name)
		return "NOAUTH Authentication required."
	}
	u := sess.user
	if u == nil {
		return ""
	}
	name := strings.ToLower(string(argv[0]))
	if msg := aclCheckUser(d, u, cmd, name, argv); msg != "" {
		reason := aclReasonFromMsg(msg)
		object := cmd.Name
		if reason == "key" || reason == "channel" {
			object = aclDeniedObject(reason, cmd, name, argv)
		}
		d.acl.addLog(c, sess, reason, "toplevel", object)
		return msg
	}
	return ""
}

// aclCheckUser checks command, key and channel permissions for a user and
// returns the NOPERM string on the first failure, or "" when allowed. It does
// not log; callers that need a log entry do so from the returned reason.
func aclCheckUser(d *Dispatcher, u *aclUser, cmd *CmdDesc, name string, argv [][]byte) string {
	if !aclCommandAllowed(u, cmd) {
		full := name
		if cmd.SubName != "" {
			full = cmd.SubName
		}
		return fmt.Sprintf("NOPERM User %s has no permissions to run the '%s' command", u.name, full)
	}

	access := aclRead
	if cmd.Flags.Has(FlagWrite) {
		access = aclWrite
	}
	if keys, ok := extractKeys(name, cmd, argv); ok {
		for _, k := range keys {
			if !aclKeyAllowed(u, string(k), access) {
				return "NOPERM No permissions to access a key"
			}
		}
	}

	// Channel checks key off the command name, not FlagPubSub, because PUBLISH and
	// SPUBLISH deliberately do not carry that flag (so they queue inside MULTI).
	for _, ch := range extractChannels(name, argv) {
		if !aclChannelAllowed(u, ch) {
			return "NOPERM No permissions to access a channel"
		}
	}
	return ""
}

// aclReasonFromMsg maps a NOPERM message back to its log reason.
func aclReasonFromMsg(msg string) string {
	switch {
	case strings.Contains(msg, "access a key"):
		return "key"
	case strings.Contains(msg, "access a channel"):
		return "channel"
	default:
		return "cmd"
	}
}

// aclDeniedObject returns the first key or channel the user could not access, for
// the ACL log object field.
func aclDeniedObject(reason string, cmd *CmdDesc, name string, argv [][]byte) string {
	if reason == "channel" {
		chans := extractChannels(name, argv)
		if len(chans) > 0 {
			return chans[0]
		}
		return ""
	}
	keys, _ := extractKeys(name, cmd, argv)
	if len(keys) > 0 {
		return string(keys[0])
	}
	return ""
}

// extractChannels returns the channel arguments of a Pub/Sub command. PUBLISH and
// SPUBLISH name a single channel; the subscribe and unsubscribe variants name a
// list. Commands with no channel arguments return nothing.
func extractChannels(name string, argv [][]byte) []string {
	switch name {
	case "publish", "spublish":
		if len(argv) >= 2 {
			return []string{string(argv[1])}
		}
	case "subscribe", "psubscribe", "ssubscribe",
		"unsubscribe", "punsubscribe", "sunsubscribe":
		out := make([]string, 0, len(argv)-1)
		for _, a := range argv[1:] {
			out = append(out, string(a))
		}
		return out
	}
	return nil
}

// addLog records an ACL denial. It coalesces a repeat of the most recent entry
// with the same reason, object, username and client address into a rising count.
func (a *aclRegistry) addLog(c *networking.Conn, sess *session, reason, context, object string) {
	addr := ""
	info := ""
	if c != nil {
		addr = c.RemoteAddr()
		info = buildClientLine(c, time.Now())
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.log) > 0 {
		top := a.log[0]
		if top.reason == reason && top.object == object && top.username == sess.username && top.addr == addr {
			top.count++
			top.at = time.Now()
			return
		}
	}
	a.nextID++
	entry := &aclLogEntry{
		count:      1,
		reason:     reason,
		context:    context,
		object:     object,
		username:   sess.username,
		clientInfo: info,
		addr:       addr,
		at:         time.Now(),
		entryID:    a.nextID - 1,
	}
	a.log = append([]*aclLogEntry{entry}, a.log...)
	if a.logMax > 0 && len(a.log) > a.logMax {
		a.log = a.log[:a.logMax]
	}
}

// loadFile parses the aclfile into a staging map and swaps it in atomically. A
// parse error leaves the live users unchanged.
func (a *aclRegistry) loadFile() error {
	data, err := os.ReadFile(a.aclFile)
	if err != nil {
		return err
	}
	staging := map[string]*aclUser{}
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "user" {
			return fmt.Errorf("%s:%d: Line should start with 'user' followed by a username", a.aclFile, i+1)
		}
		name := fields[1]
		u := &aclUser{name: name, created: time.Now()}
		if err := applyACLRules(u, fields[2:]); err != nil {
			return fmt.Errorf("%s:%d: %s", a.aclFile, i+1, err.Error())
		}
		staging[name] = u
	}
	if _, ok := staging["default"]; !ok {
		staging["default"] = &aclUser{
			name: "default", on: true, nopass: true,
			cmdRules:  []aclCmdRule{{grant: true, category: "@all"}},
			keyRules:  []aclKeyRule{{pattern: "*", read: true, write: true}},
			chanRules: []aclChanRule{{pattern: "*"}},
			created:   time.Now(),
		}
	}
	a.mu.Lock()
	a.users = staging
	a.mu.Unlock()
	return nil
}

// saveFile writes the current users to the aclfile through a temp file and a
// rename so a reader never sees a half-written file.
func (a *aclRegistry) saveFile() error {
	a.mu.RLock()
	names := make([]string, 0, len(a.users))
	for n := range a.users {
		names = append(names, n)
	}
	lines := make([]string, 0, len(names))
	for _, n := range names {
		lines = append(lines, aclLine(a.users[n]))
	}
	a.mu.RUnlock()

	sortStrings(lines)
	body := strings.Join(lines, "\n") + "\n"
	dir := filepath.Dir(a.aclFile)
	tmp, err := os.CreateTemp(dir, ".aki-acl-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, a.aclFile)
}

// sortStrings sorts a slice in place. It keeps the aclfile output stable.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
