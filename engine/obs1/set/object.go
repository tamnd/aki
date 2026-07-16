package set

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// Object answers OBJECT ENCODING key (spec 2064/f3/11 section 3): the storage
// encoding a set reports, which is what the differential test checks against
// Redis. A set answers intset, listpack, or hashtable per its live band; a
// key the string store owns gets a best-effort string encoding; a key that
// exists nowhere answers nil, which is what the redis 8.8.0 build returns for
// OBJECT ENCODING on a missing key (a null bulk, not an error; verified live).
// Only the ENCODING subcommand is wired in this slice; the others (REFCOUNT,
// IDLETIME, FREQ) return the standard unknown-subcommand error.
func Object(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if !eqFold(args[0], "ENCODING") || len(args) != 2 {
		r.Err("ERR Unknown OBJECT subcommand or wrong number of arguments. Try OBJECT HELP.")
		return
	}
	key := args[1]
	if s := registry(cx).m[string(key)]; s != nil {
		r.Bulk([]byte(s.enc.String()))
		return
	}
	// Not a set. Fall back to the string store's encoding so OBJECT ENCODING
	// answers for the type that does exist. The int/embstr/raw split is the
	// Redis default (embstr at or under 44 bytes); the raw-sticky bit APPEND
	// and SETRANGE set is not yet exposed by the store, so a short string those
	// touched reports embstr here where Redis reports raw. That gap closes with
	// the string OBJECT shim; this slice's differential test covers the set
	// encodings, which are exact.
	v, ok := cx.St.GetString(key, cx.NowMs, cx.Val)
	cx.Val = v
	if !ok {
		// The key exists in no store: redis 8.8.0 answers a null bulk here, not
		// an error.
		r.Null()
		return
	}
	r.Bulk([]byte(stringEncoding(v)))
}

func stringEncoding(v []byte) string {
	if _, ok := store.ParseInt(v); ok {
		return "int"
	}
	if len(v) <= 44 {
		return "embstr"
	}
	return "raw"
}
