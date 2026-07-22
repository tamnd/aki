package dispatch

import (
	"crypto/rand"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The ACL surface (spec 2064/f3/11, the M11 command-closure milestone). f3 runs
// no authentication or per-user access control: every connection is the one
// built-in superuser, unauthenticated, with the run of the keyspace. The
// read-only ACL verbs describe exactly that state so a client or an admin tool
// can introspect it, WHOAMI, USERS, LIST, GETUSER, and the category vocabulary
// through CAT.
//
// The mutating verbs (SETUSER, DELUSER, LOAD, SAVE, RESET) and the per-command
// category listing (CAT with an argument) are not modeled, because f3 has no
// user store and does not tag commands with ACL categories. They answer a clear
// error rather than a fabricated success, so a client never believes it created
// a user or read a category membership that f3 does not hold.

// the default superuser's fields, shared by LIST and GETUSER so the two never
// drift. f3's one user is on, passwordless, and unrestricted.
const (
	aclUserName     = "default"
	aclUserCommands = "+@all"
	aclUserKeys     = "~*"
	aclUserChannels = "&*"
	// aclListLine is the ACL LIST rendering of the default user, the same rule
	// text Redis prints for an unrestricted passwordless superuser.
	aclListLine = "user default on nopass ~* &* +@all"
)

// aclCategories is the ACL category vocabulary ACL CAT reports. f3 names the
// categories a client expects to see but does not track which command belongs to
// which, so CAT answers the vocabulary and declines a per-category listing.
var aclCategories = []string{
	"keyspace", "read", "write", "set", "sortedset", "list", "hash", "string",
	"bitmap", "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow",
	"blocking", "dangerous", "connection", "transaction", "scripting",
}

// aclCmd answers the ACL family. The subcommand sits at args[0]; register bounds
// the arity so it is always present.
func aclCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch upperVerb(args[0]) {
	case "WHOAMI":
		r.Bulk([]byte(aclUserName))
	case "USERS":
		out := resp.AppendArrayHeader(cx.Aux[:0], 1)
		out = resp.AppendBulk(out, []byte(aclUserName))
		cx.Aux = out
		r.Raw(out)
	case "LIST":
		out := resp.AppendArrayHeader(cx.Aux[:0], 1)
		out = resp.AppendBulk(out, []byte(aclListLine))
		cx.Aux = out
		r.Raw(out)
	case "CAT":
		aclCat(cx, args[1:], r)
	case "GETUSER":
		aclGetUser(cx, args[1:], r)
	case "GENPASS":
		aclGenPass(cx, args[1:], r)
	default:
		r.Err("ERR Unknown ACL subcommand or wrong number of arguments")
	}
}

// aclCat answers ACL CAT. Bare, it lists the category vocabulary. With a
// category argument it would list that category's commands, which f3 does not
// track, so it declines rather than fabricate a listing.
func aclCat(cx *shard.Ctx, rest [][]byte, r shard.Reply) {
	if len(rest) > 0 {
		r.Err("ERR f3 does not track per-command ACL categories")
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], len(aclCategories))
	for _, c := range aclCategories {
		out = resp.AppendBulk(out, []byte(c))
	}
	cx.Aux = out
	r.Raw(out)
}

// aclGenPassBits is the default password strength ACL GENPASS emits with no
// argument, and aclGenPassMax the ceiling an explicit bit count may request, the
// same 256-bit default and 4096-bit cap Redis enforces.
const (
	aclGenPassBits = 256
	aclGenPassMax  = 4096
)

// aclGenPassErr is the exact wording Redis returns for an out-of-range or
// non-integer GENPASS bit count.
const aclGenPassErr = "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096 inclusive."

// aclGenHex are the lowercase hex digits GENPASS draws each password character
// from, four random bits per digit.
const aclGenHex = "0123456789abcdef"

// aclGenPass answers ACL GENPASS [bits]: a cryptographically random password of
// bits bits, rendered as ceil(bits/4) lowercase hex characters. The default is
// 256 bits (64 hex chars); an explicit count must be a positive integer up to
// 4096. Redis draws the characters from a CSPRNG, which crypto/rand mirrors, so
// the output is a fresh unpredictable secret every call.
func aclGenPass(cx *shard.Ctx, rest [][]byte, r shard.Reply) {
	bits := int64(aclGenPassBits)
	if len(rest) > 1 {
		r.Err("ERR wrong number of arguments for 'acl|genpass' command")
		return
	}
	if len(rest) == 1 {
		n, ok := store.ParseInt(rest[0])
		if !ok {
			// Redis parses the bit count with a nil message, so a non-integer falls
			// through to the generic integer error, not the GENPASS-specific one.
			r.Err("ERR value is not an integer or out of range")
			return
		}
		if n <= 0 || n > aclGenPassMax {
			r.Err(aclGenPassErr)
			return
		}
		bits = n
	}
	// One hex character carries four bits, so a password of bits bits is
	// ceil(bits/4) characters, matching Redis's (bits+3)/4.
	chars := int((bits + 3) / 4)
	// Draw one random byte per character and keep its low nibble; a byte is cheaper
	// to reason about than packing two nibbles per byte and the cost is negligible
	// against the network round trip.
	buf := make([]byte, chars)
	if _, err := rand.Read(buf); err != nil {
		r.Err("ERR failed to generate password")
		return
	}
	for i := range buf {
		buf[i] = aclGenHex[buf[i]&0x0f]
	}
	r.Bulk(buf)
}

// aclGetUser answers ACL GETUSER name: the field map for the default superuser,
// a null array for any other name, since f3 holds no other user. The map shape
// matches Redis: flags, passwords, commands, keys, channels, selectors.
func aclGetUser(cx *shard.Ctx, rest [][]byte, r shard.Reply) {
	if len(rest) != 1 {
		r.Err("ERR wrong number of arguments for 'acl|getuser' command")
		return
	}
	if lowerASCII(rest[0]) != aclUserName {
		r.Null()
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], 12)
	out = resp.AppendBulk(out, []byte("flags"))
	out = resp.AppendArrayHeader(out, 4)
	for _, f := range []string{"on", "allkeys", "allchannels", "nopass"} {
		out = resp.AppendBulk(out, []byte(f))
	}
	out = resp.AppendBulk(out, []byte("passwords"))
	out = resp.AppendArrayHeader(out, 0)
	out = resp.AppendBulk(out, []byte("commands"))
	out = resp.AppendBulk(out, []byte(aclUserCommands))
	out = resp.AppendBulk(out, []byte("keys"))
	out = resp.AppendBulk(out, []byte(aclUserKeys))
	out = resp.AppendBulk(out, []byte("channels"))
	out = resp.AppendBulk(out, []byte(aclUserChannels))
	out = resp.AppendBulk(out, []byte("selectors"))
	out = resp.AppendArrayHeader(out, 0)
	cx.Aux = out
	r.Raw(out)
}
