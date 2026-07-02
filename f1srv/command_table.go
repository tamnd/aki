package f1srv

// This file is the COMMAND introspection surface. A client library probes COMMAND, COMMAND COUNT,
// COMMAND INFO, COMMAND DOCS, and COMMAND GETKEYS on connect to learn the command table and how to
// extract key names for routing, so answering all of these (as the old blanket did) with an empty
// array desynchronizes the client: COMMAND COUNT must be an integer, GETKEYS must return the keys or
// a defined error, and INFO must return the per-command spec.
//
// The table below is generated from live Redis 8.8, which is the source of truth for a command's
// arity, flags, key positions, and ACL categories; where Redis 8.8 and Valkey 9.1 differ on a flag
// (SUBSCRIBE carries denyoom on Redis but not Valkey, MULTI carries no_multi on Valkey but not
// Redis) the standing rule is to follow Redis 8.8. The table lists the commands aki answers, so
// COMMAND COUNT reports aki's own count rather than Redis's, which is the honest number.
//
// COMMAND INFO emits the 10-element array Redis returns: name, arity, flags, first-key, last-key,
// key-step, ACL categories, command tips, key-specs, and subcommands. aki reports empty key-specs
// and empty subcommands: a client that cannot read a key-spec falls back to the first-key/last-key/
// step triple, which every command here carries, so key extraction still works. The per-command
// GETKEYS logic reproduces Redis's own key finder for the range, keynum, sort, geo, and streams
// shapes, verified against live Redis 8.8 and Valkey 9.1, which agree on every GETKEYS case.

// getkeys kinds classify how a command's key names are found in its argument vector. Most commands
// are a plain range (first-key to last-key by step). The others need Redis's movable-key logic: a
// numkeys count that precedes the keys, an optional STORE keyword whose target is a key, or the
// STREAMS keyword that splits the tail into ids and keys.
const (
	gkNone       = iota // no key arguments
	gkRange             // keys at [firstKey, lastKey] stepping by keyStep (negative = argc-relative)
	gkKeynum            // a numkeys count at gkIdx, then that many keys immediately after it
	gkKeynumDest        // a destination key at arg 1, then a numkeys count at arg 2, then the keys
	gkSortStore         // source key at arg 1, plus an optional STORE <key>
	gkGeoStore          // source key at arg 1, plus an optional STORE/STOREDIST <key>
	gkStreams           // the STREAMS keyword, then the trailing args split half keys, half ids
)

// cmdSpec is one row of the command table: the identifying fields Redis reports for the command plus
// the getkeys classification used to answer COMMAND GETKEYS.
type cmdSpec struct {
	name     string
	arity    int
	flags    []string
	firstKey int
	lastKey  int
	keyStep  int
	aclCats  []string
	gk       int
	gkIdx    int
}

// cmdTable is the full set of commands aki answers, sorted by name for a deterministic COMMAND LIST
// and bare-COMMAND reply. It is generated from live Redis 8.8; see the file comment.
var cmdTable = []cmdSpec{
	{"append", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"auth", -2, []string{"noscript", "loading", "stale", "fast", "no_auth", "allow_busy"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"bgrewriteaof", 1, []string{"admin", "noscript", "no_async_loading"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"bgsave", -1, []string{"admin", "noscript", "no_async_loading"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"bitcount", -2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@bitmap", "@slow"}, gkRange, 0},
	{"bitfield", -2, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@bitmap", "@slow"}, gkRange, 0},
	{"bitfield_ro", -2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@bitmap", "@fast"}, gkRange, 0},
	{"bitop", -4, []string{"write", "denyoom"}, 2, -1, 1, []string{"@write", "@bitmap", "@slow"}, gkRange, 0},
	{"bitpos", -3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@bitmap", "@slow"}, gkRange, 0},
	{"blmove", 6, []string{"write", "denyoom", "blocking"}, 1, 2, 1, []string{"@write", "@list", "@slow", "@blocking"}, gkRange, 0},
	{"blmpop", -5, []string{"write", "blocking", "movablekeys"}, 0, 0, 0, []string{"@write", "@list", "@slow", "@blocking"}, gkKeynum, 2},
	{"blpop", -3, []string{"write", "blocking"}, 1, -2, 1, []string{"@write", "@list", "@slow", "@blocking"}, gkRange, 0},
	{"brpop", -3, []string{"write", "blocking"}, 1, -2, 1, []string{"@write", "@list", "@slow", "@blocking"}, gkRange, 0},
	{"brpoplpush", 4, []string{"write", "denyoom", "blocking"}, 1, 2, 1, []string{"@write", "@list", "@slow", "@blocking"}, gkRange, 0},
	{"bzmpop", -5, []string{"write", "blocking", "movablekeys"}, 0, 0, 0, []string{"@write", "@sortedset", "@slow", "@blocking"}, gkKeynum, 2},
	{"bzpopmax", -3, []string{"write", "blocking", "fast"}, 1, -2, 1, []string{"@write", "@sortedset", "@fast", "@blocking"}, gkRange, 0},
	{"bzpopmin", -3, []string{"write", "blocking", "fast"}, 1, -2, 1, []string{"@write", "@sortedset", "@fast", "@blocking"}, gkRange, 0},
	{"client", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"cluster", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"command", -1, []string{"loading", "stale"}, 0, 0, 0, []string{"@slow", "@connection"}, gkNone, 0},
	{"config", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"copy", -3, []string{"write", "denyoom"}, 1, 2, 1, []string{"@keyspace", "@write", "@slow"}, gkRange, 0},
	{"dbsize", 1, []string{"readonly", "fast"}, 0, 0, 0, []string{"@keyspace", "@read", "@fast"}, gkNone, 0},
	{"debug", -2, []string{"admin", "noscript", "loading", "stale"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"decr", 2, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"decrby", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"del", -2, []string{"write"}, 1, -1, 1, []string{"@keyspace", "@write", "@slow"}, gkRange, 0},
	{"discard", 1, []string{"noscript", "loading", "stale", "fast", "allow_busy"}, 0, 0, 0, []string{"@fast", "@transaction"}, gkNone, 0},
	{"dump", 2, []string{"readonly"}, 1, 1, 1, []string{"@keyspace", "@read", "@slow"}, gkRange, 0},
	{"echo", 2, []string{"loading", "stale", "fast"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"exec", 1, []string{"noscript", "loading", "stale", "skip_slowlog"}, 0, 0, 0, []string{"@slow", "@transaction"}, gkNone, 0},
	{"exists", -2, []string{"readonly", "fast"}, 1, -1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"expire", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"expireat", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"expiretime", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"failover", -1, []string{"admin", "noscript", "stale"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"flushall", -1, []string{"write"}, 0, 0, 0, []string{"@keyspace", "@write", "@slow", "@dangerous"}, gkNone, 0},
	{"flushdb", -1, []string{"write"}, 0, 0, 0, []string{"@keyspace", "@write", "@slow", "@dangerous"}, gkNone, 0},
	{"geoadd", -5, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@geo", "@slow"}, gkRange, 0},
	{"geodist", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@geo", "@slow"}, gkRange, 0},
	{"geohash", -2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@geo", "@slow"}, gkRange, 0},
	{"geopos", -2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@geo", "@slow"}, gkRange, 0},
	{"georadius", -6, []string{"write", "denyoom", "movablekeys"}, 1, 1, 1, []string{"@write", "@geo", "@slow"}, gkGeoStore, 0},
	{"georadius_ro", -6, []string{"readonly"}, 1, 1, 1, []string{"@read", "@geo", "@slow"}, gkGeoStore, 0},
	{"georadiusbymember", -5, []string{"write", "denyoom", "movablekeys"}, 1, 1, 1, []string{"@write", "@geo", "@slow"}, gkGeoStore, 0},
	{"georadiusbymember_ro", -5, []string{"readonly"}, 1, 1, 1, []string{"@read", "@geo", "@slow"}, gkGeoStore, 0},
	{"geosearch", -7, []string{"readonly"}, 1, 1, 1, []string{"@read", "@geo", "@slow"}, gkRange, 0},
	{"geosearchstore", -8, []string{"write", "denyoom"}, 1, 2, 1, []string{"@write", "@geo", "@slow"}, gkRange, 0},
	{"get", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@string", "@fast"}, gkRange, 0},
	{"getbit", 3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@bitmap", "@fast"}, gkRange, 0},
	{"getdel", 2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"getex", -2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"getrange", 4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@string", "@slow"}, gkRange, 0},
	{"getset", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"hdel", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hello", -1, []string{"noscript", "loading", "stale", "fast", "no_auth", "allow_busy"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"hexists", 3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hexpire", -6, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hexpireat", -6, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hexpiretime", -5, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hget", 3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hgetall", 2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@hash", "@slow"}, gkRange, 0},
	{"hgetdel", -5, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hgetex", -5, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hincrby", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hincrbyfloat", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hkeys", 2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@hash", "@slow"}, gkRange, 0},
	{"hlen", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hmget", -3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hmset", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hpersist", -5, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hpexpire", -6, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hpexpireat", -6, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hpexpiretime", -5, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hpttl", -5, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hrandfield", -2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@hash", "@slow"}, gkRange, 0},
	{"hscan", -3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@hash", "@slow"}, gkRange, 0},
	{"hset", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hsetnx", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@hash", "@fast"}, gkRange, 0},
	{"hstrlen", 3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"httl", -5, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@hash", "@fast"}, gkRange, 0},
	{"hvals", 2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@hash", "@slow"}, gkRange, 0},
	{"incr", 2, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"incrby", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"incrbyfloat", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"info", -1, []string{"loading", "stale"}, 0, 0, 0, []string{"@slow", "@dangerous"}, gkNone, 0},
	{"keys", 2, []string{"readonly"}, 0, 0, 0, []string{"@keyspace", "@read", "@slow", "@dangerous"}, gkNone, 0},
	{"lastsave", 1, []string{"loading", "stale", "fast"}, 0, 0, 0, []string{"@admin", "@fast", "@dangerous"}, gkNone, 0},
	{"latency", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"lcs", -3, []string{"readonly"}, 1, 2, 1, []string{"@read", "@string", "@slow"}, gkRange, 0},
	{"lindex", 3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@list", "@slow"}, gkRange, 0},
	{"linsert", 5, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@list", "@slow"}, gkRange, 0},
	{"llen", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@list", "@fast"}, gkRange, 0},
	{"lmove", 5, []string{"write", "denyoom"}, 1, 2, 1, []string{"@write", "@list", "@slow"}, gkRange, 0},
	{"lmpop", -4, []string{"write", "movablekeys"}, 0, 0, 0, []string{"@write", "@list", "@slow"}, gkKeynum, 1},
	{"lolwut", -1, []string{"readonly", "fast"}, 0, 0, 0, []string{"@read", "@fast"}, gkNone, 0},
	{"lpop", -2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@list", "@fast"}, gkRange, 0},
	{"lpos", -3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@list", "@slow"}, gkRange, 0},
	{"lpush", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@list", "@fast"}, gkRange, 0},
	{"lpushx", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@list", "@fast"}, gkRange, 0},
	{"lrange", 4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@list", "@slow"}, gkRange, 0},
	{"lrem", 4, []string{"write"}, 1, 1, 1, []string{"@write", "@list", "@slow"}, gkRange, 0},
	{"lset", 4, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@list", "@slow"}, gkRange, 0},
	{"ltrim", 4, []string{"write"}, 1, 1, 1, []string{"@write", "@list", "@slow"}, gkRange, 0},
	{"memory", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"mget", -2, []string{"readonly", "fast"}, 1, -1, 1, []string{"@read", "@string", "@fast"}, gkRange, 0},
	{"monitor", 1, []string{"admin", "noscript", "loading", "stale"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"mset", -3, []string{"write", "denyoom"}, 1, -1, 2, []string{"@write", "@string", "@slow"}, gkRange, 0},
	{"msetnx", -3, []string{"write", "denyoom"}, 1, -1, 2, []string{"@write", "@string", "@slow"}, gkRange, 0},
	{"multi", 1, []string{"noscript", "loading", "stale", "fast", "allow_busy"}, 0, 0, 0, []string{"@fast", "@transaction"}, gkNone, 0},
	{"object", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"persist", 2, []string{"write", "fast"}, 1, 1, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"pexpire", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"pexpireat", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"pexpiretime", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"pfadd", -2, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@hyperloglog", "@fast"}, gkRange, 0},
	{"pfcount", -2, []string{"readonly"}, 1, -1, 1, []string{"@read", "@hyperloglog", "@slow"}, gkRange, 0},
	{"pfmerge", -2, []string{"write", "denyoom"}, 1, -1, 1, []string{"@write", "@hyperloglog", "@slow"}, gkRange, 0},
	{"ping", -1, []string{"fast"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"psetex", 4, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@string", "@slow"}, gkRange, 0},
	{"psubscribe", -2, []string{"denyoom", "pubsub", "noscript", "loading", "stale"}, 0, 0, 0, []string{"@pubsub", "@slow"}, gkNone, 0},
	{"pttl", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"publish", 3, []string{"pubsub", "loading", "stale", "fast"}, 0, 0, 0, []string{"@pubsub", "@fast"}, gkNone, 0},
	{"pubsub", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"punsubscribe", -1, []string{"pubsub", "noscript", "loading", "stale"}, 0, 0, 0, []string{"@pubsub", "@slow"}, gkNone, 0},
	{"quit", -1, []string{"noscript", "loading", "stale", "fast", "no_auth", "allow_busy"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"randomkey", 1, []string{"readonly"}, 0, 0, 0, []string{"@keyspace", "@read", "@slow"}, gkNone, 0},
	{"rename", 3, []string{"write"}, 1, 2, 1, []string{"@keyspace", "@write", "@slow"}, gkRange, 0},
	{"renamenx", 3, []string{"write", "fast"}, 1, 2, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"replicaof", 3, []string{"admin", "noscript", "stale", "no_async_loading"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"reset", 1, []string{"noscript", "loading", "stale", "fast", "no_auth", "allow_busy"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"restore", -4, []string{"write", "denyoom"}, 1, 1, 1, []string{"@keyspace", "@write", "@slow", "@dangerous"}, gkRange, 0},
	{"role", 1, []string{"noscript", "loading", "stale", "fast"}, 0, 0, 0, []string{"@admin", "@fast", "@dangerous"}, gkNone, 0},
	{"rpop", -2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@list", "@fast"}, gkRange, 0},
	{"rpoplpush", 3, []string{"write", "denyoom"}, 1, 2, 1, []string{"@write", "@list", "@slow"}, gkRange, 0},
	{"rpush", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@list", "@fast"}, gkRange, 0},
	{"rpushx", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@list", "@fast"}, gkRange, 0},
	{"sadd", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@set", "@fast"}, gkRange, 0},
	{"save", 1, []string{"admin", "noscript", "no_async_loading", "no_multi"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"scan", -2, []string{"readonly"}, 0, 0, 0, []string{"@keyspace", "@read", "@slow"}, gkNone, 0},
	{"scard", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@set", "@fast"}, gkRange, 0},
	{"sdiff", -2, []string{"readonly"}, 1, -1, 1, []string{"@read", "@set", "@slow"}, gkRange, 0},
	{"sdiffstore", -3, []string{"write", "denyoom"}, 1, -1, 1, []string{"@write", "@set", "@slow"}, gkRange, 0},
	{"select", 2, []string{"loading", "stale", "fast"}, 0, 0, 0, []string{"@fast", "@connection"}, gkNone, 0},
	{"set", -3, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@string", "@slow"}, gkRange, 0},
	{"setbit", 4, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@bitmap", "@slow"}, gkRange, 0},
	{"setex", 4, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@string", "@slow"}, gkRange, 0},
	{"setnx", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@string", "@fast"}, gkRange, 0},
	{"setrange", 4, []string{"write", "denyoom"}, 1, 1, 1, []string{"@write", "@string", "@slow"}, gkRange, 0},
	{"shutdown", -1, []string{"admin", "noscript", "loading", "stale", "no_multi", "allow_busy"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"sinter", -2, []string{"readonly"}, 1, -1, 1, []string{"@read", "@set", "@slow"}, gkRange, 0},
	{"sintercard", -3, []string{"readonly", "movablekeys"}, 0, 0, 0, []string{"@read", "@set", "@slow"}, gkKeynum, 1},
	{"sinterstore", -3, []string{"write", "denyoom"}, 1, -1, 1, []string{"@write", "@set", "@slow"}, gkRange, 0},
	{"sismember", 3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@set", "@fast"}, gkRange, 0},
	{"slaveof", 3, []string{"admin", "noscript", "stale", "no_async_loading"}, 0, 0, 0, []string{"@admin", "@slow", "@dangerous"}, gkNone, 0},
	{"slowlog", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"smembers", 2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@set", "@slow"}, gkRange, 0},
	{"smismember", -3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@set", "@fast"}, gkRange, 0},
	{"smove", 4, []string{"write", "fast"}, 1, 2, 1, []string{"@write", "@set", "@fast"}, gkRange, 0},
	{"sort", -2, []string{"write", "denyoom", "movablekeys"}, 1, 1, 1, []string{"@write", "@set", "@sortedset", "@list", "@slow", "@dangerous"}, gkSortStore, 0},
	{"sort_ro", -2, []string{"readonly", "movablekeys"}, 1, 1, 1, []string{"@read", "@set", "@sortedset", "@list", "@slow", "@dangerous"}, gkSortStore, 0},
	{"spop", -2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@set", "@fast"}, gkRange, 0},
	{"spublish", 3, []string{"pubsub", "loading", "stale", "fast"}, 1, 1, 1, []string{"@pubsub", "@fast"}, gkRange, 0},
	{"srandmember", -2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@set", "@slow"}, gkRange, 0},
	{"srem", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@set", "@fast"}, gkRange, 0},
	{"sscan", -3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@set", "@slow"}, gkRange, 0},
	{"ssubscribe", -2, []string{"denyoom", "pubsub", "noscript", "loading", "stale"}, 1, -1, 1, []string{"@pubsub", "@slow"}, gkRange, 0},
	{"strlen", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@string", "@fast"}, gkRange, 0},
	{"subscribe", -2, []string{"denyoom", "pubsub", "noscript", "loading", "stale"}, 0, 0, 0, []string{"@pubsub", "@slow"}, gkNone, 0},
	{"substr", 4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@string", "@slow"}, gkRange, 0},
	{"sunion", -2, []string{"readonly"}, 1, -1, 1, []string{"@read", "@set", "@slow"}, gkRange, 0},
	{"sunionstore", -3, []string{"write", "denyoom"}, 1, -1, 1, []string{"@write", "@set", "@slow"}, gkRange, 0},
	{"sunsubscribe", -1, []string{"pubsub", "noscript", "loading", "stale"}, 1, -1, 1, []string{"@pubsub", "@slow"}, gkRange, 0},
	{"swapdb", 3, []string{"write", "fast"}, 0, 0, 0, []string{"@keyspace", "@write", "@fast", "@dangerous"}, gkNone, 0},
	{"time", 1, []string{"loading", "stale", "fast"}, 0, 0, 0, []string{"@fast"}, gkNone, 0},
	{"touch", -2, []string{"readonly", "fast"}, 1, -1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"ttl", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"type", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@keyspace", "@read", "@fast"}, gkRange, 0},
	{"unlink", -2, []string{"write", "fast"}, 1, -1, 1, []string{"@keyspace", "@write", "@fast"}, gkRange, 0},
	{"unsubscribe", -1, []string{"pubsub", "noscript", "loading", "stale"}, 0, 0, 0, []string{"@pubsub", "@slow"}, gkNone, 0},
	{"unwatch", 1, []string{"noscript", "loading", "stale", "fast", "allow_busy"}, 0, 0, 0, []string{"@fast", "@transaction"}, gkNone, 0},
	{"wait", 3, []string{"blocking"}, 0, 0, 0, []string{"@slow", "@blocking", "@connection"}, gkNone, 0},
	{"waitaof", 4, []string{"blocking"}, 0, 0, 0, []string{"@slow", "@blocking", "@connection"}, gkNone, 0},
	{"watch", -2, []string{"noscript", "loading", "stale", "fast", "allow_busy"}, 1, -1, 1, []string{"@fast", "@transaction"}, gkRange, 0},
	{"xack", -4, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@stream", "@fast"}, gkRange, 0},
	{"xadd", -5, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@stream", "@fast"}, gkRange, 0},
	{"xautoclaim", -6, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@stream", "@fast"}, gkRange, 0},
	{"xclaim", -6, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@stream", "@fast"}, gkRange, 0},
	{"xdel", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@stream", "@fast"}, gkRange, 0},
	{"xgroup", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"xinfo", -2, []string{}, 0, 0, 0, []string{"@slow"}, gkNone, 0},
	{"xlen", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@stream", "@fast"}, gkRange, 0},
	{"xpending", -3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@stream", "@slow"}, gkRange, 0},
	{"xrange", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@stream", "@slow"}, gkRange, 0},
	{"xread", -4, []string{"readonly", "blocking", "movablekeys"}, 0, 0, 0, []string{"@read", "@stream", "@slow", "@blocking"}, gkStreams, 0},
	{"xreadgroup", -7, []string{"write", "blocking", "movablekeys"}, 0, 0, 0, []string{"@write", "@stream", "@slow", "@blocking"}, gkStreams, 0},
	{"xrevrange", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@stream", "@slow"}, gkRange, 0},
	{"xsetid", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@stream", "@fast"}, gkRange, 0},
	{"xtrim", -4, []string{"write"}, 1, 1, 1, []string{"@write", "@stream", "@slow"}, gkRange, 0},
	{"zadd", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@sortedset", "@fast"}, gkRange, 0},
	{"zcard", 2, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zcount", 4, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zdiff", -3, []string{"readonly", "movablekeys"}, 0, 0, 0, []string{"@read", "@sortedset", "@slow"}, gkKeynum, 1},
	{"zdiffstore", -4, []string{"write", "denyoom", "movablekeys"}, 1, 1, 1, []string{"@write", "@sortedset", "@slow"}, gkKeynumDest, 0},
	{"zincrby", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, []string{"@write", "@sortedset", "@fast"}, gkRange, 0},
	{"zinter", -3, []string{"readonly", "movablekeys"}, 0, 0, 0, []string{"@read", "@sortedset", "@slow"}, gkKeynum, 1},
	{"zintercard", -3, []string{"readonly", "movablekeys"}, 0, 0, 0, []string{"@read", "@sortedset", "@slow"}, gkKeynum, 1},
	{"zinterstore", -4, []string{"write", "denyoom", "movablekeys"}, 1, 1, 1, []string{"@write", "@sortedset", "@slow"}, gkKeynumDest, 0},
	{"zlexcount", 4, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zmpop", -4, []string{"write", "movablekeys"}, 0, 0, 0, []string{"@write", "@sortedset", "@slow"}, gkKeynum, 1},
	{"zmscore", -3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zpopmax", -2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@sortedset", "@fast"}, gkRange, 0},
	{"zpopmin", -2, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@sortedset", "@fast"}, gkRange, 0},
	{"zrandmember", -2, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrange", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrangebylex", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrangebyscore", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrangestore", -5, []string{"write", "denyoom"}, 1, 2, 1, []string{"@write", "@sortedset", "@slow"}, gkRange, 0},
	{"zrank", -3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zrem", -3, []string{"write", "fast"}, 1, 1, 1, []string{"@write", "@sortedset", "@fast"}, gkRange, 0},
	{"zremrangebylex", 4, []string{"write"}, 1, 1, 1, []string{"@write", "@sortedset", "@slow"}, gkRange, 0},
	{"zremrangebyrank", 4, []string{"write"}, 1, 1, 1, []string{"@write", "@sortedset", "@slow"}, gkRange, 0},
	{"zremrangebyscore", 4, []string{"write"}, 1, 1, 1, []string{"@write", "@sortedset", "@slow"}, gkRange, 0},
	{"zrevrange", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrevrangebylex", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrevrangebyscore", -4, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zrevrank", -3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zscan", -3, []string{"readonly"}, 1, 1, 1, []string{"@read", "@sortedset", "@slow"}, gkRange, 0},
	{"zscore", 3, []string{"readonly", "fast"}, 1, 1, 1, []string{"@read", "@sortedset", "@fast"}, gkRange, 0},
	{"zunion", -3, []string{"readonly", "movablekeys"}, 0, 0, 0, []string{"@read", "@sortedset", "@slow"}, gkKeynum, 1},
	{"zunionstore", -4, []string{"write", "denyoom", "movablekeys"}, 1, 1, 1, []string{"@write", "@sortedset", "@slow"}, gkKeynumDest, 0},
}

// cmdByName indexes cmdTable by command name for O(1) COMMAND INFO and GETKEYS lookups.
var cmdByName = func() map[string]*cmdSpec {
	m := make(map[string]*cmdSpec, len(cmdTable))
	for i := range cmdTable {
		m[cmdTable[i].name] = &cmdTable[i]
	}
	return m
}()
