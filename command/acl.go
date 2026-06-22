package command

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"
)

// This file implements the ACL user model and the permission checks the dispatch
// pipeline runs (spec 2064 doc 19 sections 11 and 13). Users are kept in a small
// registry guarded by a lock. Every connection points at one user; the default
// user covers connections that never run AUTH. Rules are parsed from the same
// token syntax ACL SETUSER and the aclfile accept, and the command, key and
// channel checks follow the pseudocode in doc 19 section 13.

// aclCategoryNames is the canonical list of ACL category names, without the @
// prefix, in the order ACL CAT reports them. They line up with the categories
// commandCategories derives from a command's group and flags.
var aclCategoryNames = []string{
	"keyspace", "read", "write", "set", "sortedset", "list", "hash",
	"string", "bitmap", "hyperloglog", "geo", "stream", "pubsub",
	"admin", "fast", "slow", "blocking", "dangerous", "connection",
	"transaction", "scripting",
}

// aclAccess is the access a command needs on a key: read or write.
type aclAccess int

const (
	aclRead aclAccess = iota
	aclWrite
)

// aclCmdRule is one command permission rule. A category rule names a category
// like "@all" or "@read"; otherwise it names a command, optionally with a
// subcommand. grant is true for a "+" rule and false for a "-" rule.
type aclCmdRule struct {
	grant    bool
	category string
	cmd      string
	sub      string
}

// token renders the rule back into the "+get" or "-@admin" form used by ACL LIST
// and the aclfile.
func (r aclCmdRule) token() string {
	sign := "+"
	if !r.grant {
		sign = "-"
	}
	if r.category != "" {
		return sign + r.category
	}
	if r.sub != "" {
		return sign + r.cmd + "|" + r.sub
	}
	return sign + r.cmd
}

// aclKeyRule is one key permission rule: a glob pattern and the access it grants.
type aclKeyRule struct {
	pattern string
	read    bool
	write   bool
}

// token renders the key rule back into "~pat", "%R~pat", "%W~pat" or "%RW~pat".
func (r aclKeyRule) token() string {
	switch {
	case r.read && r.write:
		return "~" + r.pattern
	case r.read:
		return "%R~" + r.pattern
	default:
		return "%W~" + r.pattern
	}
}

// aclChanRule is one channel permission rule: a glob pattern.
type aclChanRule struct {
	pattern string
}

// aclSelector is a compound rule unit added with the (rules...) syntax. It holds
// its own command, key and channel rules and is evaluated as an alternative to
// the user's root rules.
type aclSelector struct {
	cmdRules  []aclCmdRule
	keyRules  []aclKeyRule
	chanRules []aclChanRule
}

// aclUser is one ACL user: its status, passwords and permission rules.
type aclUser struct {
	name      string
	on        bool
	nopass    bool
	passwords []string // sha256 hex hashes
	cmdRules  []aclCmdRule
	keyRules  []aclKeyRule
	chanRules []aclChanRule
	selectors []aclSelector
	created   time.Time
}

// allKeys reports whether the user has a ~* read+write rule, the allkeys flag.
func (u *aclUser) allKeys() bool {
	for _, r := range u.keyRules {
		if r.pattern == "*" && r.read && r.write {
			return true
		}
	}
	return false
}

// allChannels reports whether the user has a &* rule, the allchannels flag.
func (u *aclUser) allChannels() bool {
	for _, r := range u.chanRules {
		if r.pattern == "*" {
			return true
		}
	}
	return false
}

// reset returns the user to the initial state: off, no passwords, no rules.
func (u *aclUser) reset() {
	u.on = false
	u.nopass = false
	u.passwords = nil
	u.cmdRules = nil
	u.keyRules = nil
	u.chanRules = nil
	u.selectors = nil
}

// hashPassword returns the lowercase SHA-256 hex of a plaintext password, the
// form ACL stores and compares.
func hashPassword(pw string) string {
	sum := sha256.Sum256([]byte(pw))
	return hex.EncodeToString(sum[:])
}

// checkPassword reports whether pw authenticates this user. A nopass user accepts
// anything; otherwise the SHA-256 of pw must be in the password list.
func (u *aclUser) checkPassword(pw string) bool {
	if u.nopass {
		return true
	}
	want := hashPassword(pw)
	for _, h := range u.passwords {
		if h == want {
			return true
		}
	}
	return false
}

// aclRegistry holds every user and the ACL denial log. It is guarded by a lock
// because connections on different goroutines read it on every command and the
// ACL command family writes it.
type aclRegistry struct {
	mu      sync.RWMutex
	users   map[string]*aclUser
	log     []*aclLogEntry
	logMax  int
	nextID  int64
	aclFile string
}

// newACLRegistry builds a registry with the default user. When requirepass is
// set, the default user gets that password and the nopass flag is cleared, the
// legacy single-password mapping. Otherwise the default user is nopass.
func newACLRegistry(requirePass string) *aclRegistry {
	def := &aclUser{
		name:     "default",
		on:       true,
		cmdRules: []aclCmdRule{{grant: true, category: "@all"}},
		keyRules: []aclKeyRule{{pattern: "*", read: true, write: true}},
		chanRules: []aclChanRule{
			{pattern: "*"},
		},
		created: time.Now(),
	}
	if requirePass != "" {
		def.passwords = []string{hashPassword(requirePass)}
	} else {
		def.nopass = true
	}
	return &aclRegistry{
		users:  map[string]*aclUser{"default": def},
		logMax: 128,
	}
}

// get returns the user with the given name, or nil.
func (a *aclRegistry) get(name string) *aclUser {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.users[name]
}

// authenticate checks a username and password and returns the user on success.
// A missing or disabled user, or a wrong password, fails.
func (a *aclRegistry) authenticate(name, pw string) (*aclUser, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	u := a.users[name]
	if u == nil || !u.on {
		return nil, false
	}
	if !u.checkPassword(pw) {
		return nil, false
	}
	return u, true
}

// usernames returns every username sorted, which ACL USERS and ACL LIST report
// in a stable order.
func (a *aclRegistry) usernames() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]string, 0, len(a.users))
	for n := range a.users {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// del removes a user. The default user cannot be removed.
func (a *aclRegistry) del(name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if name == "default" {
		return false
	}
	if _, ok := a.users[name]; !ok {
		return false
	}
	delete(a.users, name)
	return true
}

// ruleMatchesCmd reports whether a command rule covers the resolved command. A
// category rule matches when the command belongs to the category (or the rule is
// @all). A command rule matches by parent name, and also by subcommand when the
// rule names one.
func ruleMatchesCmd(r aclCmdRule, cmd *CmdDesc) bool {
	if r.category != "" {
		if r.category == "@all" {
			return true
		}
		for _, c := range commandCategories(cmd) {
			if c == r.category {
				return true
			}
		}
		return false
	}
	parent, sub := cmdParentSub(cmd)
	if r.sub != "" {
		return r.cmd == parent && r.sub == sub
	}
	return r.cmd == parent
}

// cmdParentSub splits a resolved descriptor into its parent command name and an
// optional subcommand. A subcommand descriptor carries "parent|sub" in SubName.
func cmdParentSub(cmd *CmdDesc) (string, string) {
	if i := strings.IndexByte(cmd.SubName, '|'); i >= 0 {
		return cmd.SubName[:i], cmd.SubName[i+1:]
	}
	return cmd.Name, ""
}

// cmdRulesAllow evaluates a command rule list left to right, last match wins, and
// reports whether the command ends up allowed. An empty list denies.
func cmdRulesAllow(rules []aclCmdRule, cmd *CmdDesc) bool {
	allowed := false
	for _, r := range rules {
		if ruleMatchesCmd(r, cmd) {
			allowed = r.grant
		}
	}
	return allowed
}

// aclCommandAllowed reports whether the user may run the command, checking the
// root rules first and then each selector as an alternative.
func aclCommandAllowed(u *aclUser, cmd *CmdDesc) bool {
	if cmdRulesAllow(u.cmdRules, cmd) {
		return true
	}
	for i := range u.selectors {
		if cmdRulesAllow(u.selectors[i].cmdRules, cmd) {
			return true
		}
	}
	return false
}

// keyRulesAllow reports whether a key rule list grants the requested access to
// the key. The first matching rule that covers the access wins.
func keyRulesAllow(rules []aclKeyRule, key string, access aclAccess) bool {
	for _, r := range rules {
		if !stringMatch([]byte(r.pattern), []byte(key), false) {
			continue
		}
		if access == aclRead && r.read {
			return true
		}
		if access == aclWrite && r.write {
			return true
		}
	}
	return false
}

// aclKeyAllowed reports whether the user may access the key with the given
// access, checking the root key rules and then each selector.
func aclKeyAllowed(u *aclUser, key string, access aclAccess) bool {
	if u.allKeys() {
		return true
	}
	if keyRulesAllow(u.keyRules, key, access) {
		return true
	}
	for i := range u.selectors {
		if keyRulesAllow(u.selectors[i].keyRules, key, access) {
			return true
		}
	}
	return false
}

// aclChannelAllowed reports whether the user may use the Pub/Sub channel. Channel
// rules live at the root only.
func aclChannelAllowed(u *aclUser, channel string) bool {
	if u.allChannels() {
		return true
	}
	for _, r := range u.chanRules {
		if stringMatch([]byte(r.pattern), []byte(channel), false) {
			return true
		}
	}
	return false
}

// genPass returns a random hex password of ceil(bits/4) characters, the helper
// behind ACL GENPASS. bits must be between 1 and 4096.
func genPass(bits int) (string, bool) {
	if bits < 1 || bits > 4096 {
		return "", false
	}
	chars := (bits + 3) / 4
	buf := make([]byte, (chars+1)/2)
	if _, err := rand.Read(buf); err != nil {
		return "", false
	}
	return hex.EncodeToString(buf)[:chars], true
}
