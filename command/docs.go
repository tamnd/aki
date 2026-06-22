package command

import (
	"strings"

	"github.com/tamnd/aki/resp"
)

// This file implements COMMAND DOCS (spec 2064 doc 07 section 7.4). The reply is a
// map of command name to a documentation map with summary, since, group,
// complexity, arguments, and (for containers) subcommands. since and group come
// straight from the descriptor, so every command answers with at least those.
// summary, complexity, and the structured argument trees come from the docs
// overlay below. The overlay covers the core command set; commands without an
// entry still answer with their descriptor-derived fields, the same stance
// COMMAND INFO takes for tips and key specs it does not carry yet.

// docArg is one structured argument in a command's documentation. It mirrors the
// Redis COMMAND DOCS argument map. Token is set for pure-token arguments, and Args
// holds the nested arguments of a oneof or block.
type docArg struct {
	name        string
	typ         string // string|integer|double|key|pattern|unix-time|pure-token|oneof|block
	displayText string
	token       string
	flags       []string // optional|multiple|multiple_token
	args        []docArg
}

// cmdDoc is the overlay entry for one command: its one-line summary, its big-O
// complexity, and its structured arguments.
type cmdDoc struct {
	summary    string
	complexity string
	args       []docArg
}

// groupName maps a command group to the group string Redis reports in COMMAND
// DOCS. It is the human-facing name, not the ACL category.
var groupName = map[CmdGroup]string{
	GroupString:       "string",
	GroupBitmap:       "bitmap",
	GroupHyperLogLog:  "hyperloglog",
	GroupGeo:          "geo",
	GroupList:         "list",
	GroupSet:          "set",
	GroupSortedSet:    "sorted-set",
	GroupHash:         "hash",
	GroupStream:       "stream",
	GroupPubSub:       "pubsub",
	GroupTransactions: "transactions",
	GroupScripting:    "scripting",
	GroupGeneric:      "generic",
	GroupConnection:   "connection",
	GroupServer:       "server",
	GroupCluster:      "cluster",
}

// commandDocs is the documentation overlay keyed by command name. It carries the
// summaries and complexity strings for the core command set, plus worked argument
// trees for the string commands as the model for the structured form.
var commandDocs = map[string]cmdDoc{
	"get": {
		summary:    "Returns the string value of a key.",
		complexity: "O(1)",
		args: []docArg{
			{name: "key", typ: "key", displayText: "key"},
		},
	},
	"set": {
		summary:    "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
		complexity: "O(1)",
		args: []docArg{
			{name: "key", typ: "key", displayText: "key"},
			{name: "value", typ: "string", displayText: "value"},
			{name: "condition", typ: "oneof", flags: []string{"optional"}, args: []docArg{
				{name: "nx", typ: "pure-token", token: "NX"},
				{name: "xx", typ: "pure-token", token: "XX"},
			}},
			{name: "get", typ: "pure-token", token: "GET", flags: []string{"optional"}},
			{name: "expiration", typ: "oneof", flags: []string{"optional"}, args: []docArg{
				{name: "seconds", typ: "integer", displayText: "seconds", token: "EX"},
				{name: "milliseconds", typ: "integer", displayText: "milliseconds", token: "PX"},
				{name: "unix-time-seconds", typ: "unix-time", displayText: "unix-time-seconds", token: "EXAT"},
				{name: "unix-time-milliseconds", typ: "unix-time", displayText: "unix-time-milliseconds", token: "PXAT"},
				{name: "keepttl", typ: "pure-token", token: "KEEPTTL"},
			}},
		},
	},
	"setnx":       {summary: "Set the string value of a key only when the key doesn't exist.", complexity: "O(1)"},
	"setex":       {summary: "Sets the string value and expiration time of a key. Creates the key if it doesn't exist.", complexity: "O(1)"},
	"psetex":      {summary: "Sets the string value and expiration time in milliseconds of a key.", complexity: "O(1)"},
	"getset":      {summary: "Returns the previous string value of a key after setting it to a new value.", complexity: "O(1)"},
	"getdel":      {summary: "Returns the string value of a key after deleting the key.", complexity: "O(1)"},
	"getex":       {summary: "Returns the string value of a key after setting its expiration time.", complexity: "O(1)"},
	"append":      {summary: "Appends a string to the value of a key. Creates the key if it doesn't exist.", complexity: "O(1)"},
	"strlen":      {summary: "Returns the length of a string value.", complexity: "O(1)"},
	"getrange":    {summary: "Returns a substring of the string stored at a key.", complexity: "O(N)"},
	"setrange":    {summary: "Overwrites a part of a string value with another by an offset.", complexity: "O(1)"},
	"incr":        {summary: "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", complexity: "O(1)"},
	"decr":        {summary: "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", complexity: "O(1)"},
	"incrby":      {summary: "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.", complexity: "O(1)"},
	"decrby":      {summary: "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist.", complexity: "O(1)"},
	"incrbyfloat": {summary: "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.", complexity: "O(1)"},
	"mget":        {summary: "Atomically returns the string values of one or more keys.", complexity: "O(N) where N is the number of keys to retrieve."},
	"mset":        {summary: "Atomically creates or modifies the string values of one or more keys.", complexity: "O(N) where N is the number of keys to set."},
	"msetnx":      {summary: "Atomically modifies the string values of one or more keys only when all keys don't exist.", complexity: "O(N) where N is the number of keys to set."},

	"del":       {summary: "Deletes one or more keys.", complexity: "O(N) where N is the number of keys that will be removed."},
	"unlink":    {summary: "Asynchronously deletes one or more keys.", complexity: "O(1) for each key removed regardless of its size."},
	"exists":    {summary: "Determines whether one or more keys exist.", complexity: "O(N) where N is the number of keys to check."},
	"expire":    {summary: "Sets the expiration time of a key in seconds.", complexity: "O(1)"},
	"pexpire":   {summary: "Sets the expiration time of a key in milliseconds.", complexity: "O(1)"},
	"expireat":  {summary: "Sets the expiration time of a key to a Unix timestamp.", complexity: "O(1)"},
	"pexpireat": {summary: "Sets the expiration time of a key to a Unix milliseconds timestamp.", complexity: "O(1)"},
	"ttl":       {summary: "Returns the expiration time in seconds of a key.", complexity: "O(1)"},
	"pttl":      {summary: "Returns the expiration time in milliseconds of a key.", complexity: "O(1)"},
	"persist":   {summary: "Removes the expiration time of a key.", complexity: "O(1)"},
	"type":      {summary: "Returns the type of value stored at a key.", complexity: "O(1)"},
	"rename":    {summary: "Renames a key and overwrites the destination.", complexity: "O(1)"},
	"renamenx":  {summary: "Renames a key only when the target key name doesn't exist.", complexity: "O(1)"},
	"keys":      {summary: "Returns all key names that match a pattern.", complexity: "O(N) with N being the number of keys in the database."},
	"scan":      {summary: "Iterates over the key names in the database.", complexity: "O(1) for every call. O(N) for a complete iteration."},
	"randomkey": {summary: "Returns a random key name from the database.", complexity: "O(1)"},
	"dbsize":    {summary: "Returns the number of keys in the database.", complexity: "O(1)"},
	"copy":      {summary: "Copies the value of a key to a new key.", complexity: "O(N) worst case for collections, where N is the number of nested items."},
	"move":      {summary: "Moves a key to another database.", complexity: "O(1)"},
	"touch":     {summary: "Returns the number of existing keys out of those specified after updating the time they were last accessed.", complexity: "O(N) where N is the number of keys that will be touched."},
	"dump":      {summary: "Returns a serialized representation of the value stored at a key.", complexity: "O(1) to access the key and additional O(N*M) to serialize it."},
	"restore":   {summary: "Creates a key from the serialized representation of a value.", complexity: "O(1) to create the new key and additional O(N*M) to reconstruct the value."},

	"ping":   {summary: "Returns the server's liveliness response.", complexity: "O(1)"},
	"echo":   {summary: "Returns the given string.", complexity: "O(1)"},
	"select": {summary: "Changes the selected database.", complexity: "O(1)"},
	"hello":  {summary: "Handshakes with the Redis server.", complexity: "O(1)"},
	"auth":   {summary: "Authenticates the connection.", complexity: "O(N) where N is the number of passwords defined for the user."},
	"quit":   {summary: "Closes the connection.", complexity: "O(1)"},
	"reset":  {summary: "Resets the connection.", complexity: "O(1)"},

	"info":     {summary: "Returns information and statistics about the server.", complexity: "O(1)"},
	"dbsize ":  {summary: "Returns the number of keys in the database.", complexity: "O(1)"},
	"command":  {summary: "Returns detailed information about all commands.", complexity: "O(N) where N is the total number of Redis commands."},
	"config":   {summary: "A container for server configuration commands.", complexity: "Depends on subcommand."},
	"client":   {summary: "A container for client connection commands.", complexity: "Depends on subcommand."},
	"shutdown": {summary: "Synchronously saves the dataset to disk and then shuts down the server.", complexity: "O(N) when saving, where N is the total number of keys."},
	"flushdb":  {summary: "Removes all keys from the current database.", complexity: "O(N) where N is the number of keys in the database."},
	"flushall": {summary: "Removes all keys from all databases.", complexity: "O(N) where N is the total number of keys in all databases."},
	"save":     {summary: "Synchronously saves the database to disk.", complexity: "O(N) where N is the total number of keys."},
	"bgsave":   {summary: "Asynchronously saves the database to disk.", complexity: "O(N) where N is the total number of keys."},
	"lastsave": {summary: "Returns the Unix timestamp of the last successful save to disk.", complexity: "O(1)"},
	"wait":     {summary: "Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed.", complexity: "O(1)"},
	"waitaof":  {summary: "Blocks until all of the preceding write commands sent by the connection are written to the append-only file of the master and/or replicas.", complexity: "O(1)"},

	"lpush":  {summary: "Prepends one or more elements to a list. Creates the key if it doesn't exist.", complexity: "O(1) for each element added."},
	"rpush":  {summary: "Appends one or more elements to a list. Creates the key if it doesn't exist.", complexity: "O(1) for each element added."},
	"lpop":   {summary: "Returns the first elements in a list after removing it. Deletes the list if the last element was popped.", complexity: "O(N) where N is the number of elements returned."},
	"rpop":   {summary: "Returns and removes the last elements of a list. Deletes the list if the last element was popped.", complexity: "O(N) where N is the number of elements returned."},
	"llen":   {summary: "Returns the length of a list.", complexity: "O(1)"},
	"lrange": {summary: "Returns a range of elements from a list.", complexity: "O(S+N) where S is the offset and N the number of elements in the range."},
	"lindex": {summary: "Returns an element from a list by its index.", complexity: "O(N) where N is the number of elements to traverse."},
	"lset":   {summary: "Sets the value of an element in a list by its index.", complexity: "O(N) where N is the length of the list."},

	"hset":    {summary: "Creates or modifies the value of a field in a hash.", complexity: "O(1) for each field/value pair added."},
	"hget":    {summary: "Returns the value of a field in a hash.", complexity: "O(1)"},
	"hdel":    {summary: "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.", complexity: "O(N) where N is the number of fields to be removed."},
	"hgetall": {summary: "Returns all fields and values in a hash.", complexity: "O(N) where N is the size of the hash."},
	"hlen":    {summary: "Returns the number of fields in a hash.", complexity: "O(1)"},

	"sadd":      {summary: "Adds one or more members to a set. Creates the key if it doesn't exist.", complexity: "O(1) for each element added."},
	"srem":      {summary: "Removes one or more members from a set. Deletes the set if the last member was removed.", complexity: "O(N) where N is the number of members to be removed."},
	"smembers":  {summary: "Returns all members of a set.", complexity: "O(N) where N is the set cardinality."},
	"scard":     {summary: "Returns the number of members in a set.", complexity: "O(1)"},
	"sismember": {summary: "Determines whether a member belongs to a set.", complexity: "O(1)"},

	"zadd":   {summary: "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist.", complexity: "O(log(N)) for each item added, where N is the number of elements in the sorted set."},
	"zrange": {summary: "Returns members in a sorted set within a range of indexes.", complexity: "O(log(N)+M) with N elements in the sorted set and M elements returned."},
	"zscore": {summary: "Returns the score of a member in a sorted set.", complexity: "O(1)"},
	"zcard":  {summary: "Returns the number of members in a sorted set.", complexity: "O(1)"},

	"subscribe":   {summary: "Listens for messages published to channels.", complexity: "O(N) where N is the number of channels to subscribe to."},
	"unsubscribe": {summary: "Stops listening to messages posted to channels.", complexity: "O(N) where N is the number of channels to unsubscribe."},
	"publish":     {summary: "Posts a message to a channel.", complexity: "O(N+M) where N is the number of clients subscribed to the receiving channel and M is the total number of subscribed patterns."},

	"multi":   {summary: "Starts a transaction.", complexity: "O(1)"},
	"exec":    {summary: "Executes all commands in a transaction.", complexity: "Depends on commands in the transaction."},
	"discard": {summary: "Discards a transaction.", complexity: "O(N), when N is the number of queued commands."},
	"watch":   {summary: "Monitors changes to keys to determine the execution of a transaction.", complexity: "O(1) for every key."},
	"unwatch": {summary: "Forgets about watched keys of a transaction.", complexity: "O(1)"},

	"eval":    {summary: "Executes a server-side Lua script.", complexity: "Depends on the script that is executed."},
	"evalsha": {summary: "Executes a server-side Lua script by SHA1 digest.", complexity: "Depends on the script that is executed."},
}

// docFor returns the overlay entry for a command, looking up its full name. A
// subcommand is keyed by its container|sub name, falling back to the bare sub name.
func docFor(cmd *CmdDesc) cmdDoc {
	if cmd.SubName != "" {
		if d, ok := commandDocs[cmd.SubName]; ok {
			return d
		}
	}
	return commandDocs[cmd.Name]
}

// handleCommandDocs returns the documentation map for the requested commands, or
// every command when none are named. Unknown names are omitted, so the reply map
// only counts the commands that resolved.
func handleCommandDocs(ctx *Ctx) {
	enc := ctx.enc()
	if len(ctx.Argv) == 2 {
		cmds := ctx.d.table.commands()
		enc.WriteMapLen(len(cmds))
		for _, c := range cmds {
			enc.WriteBulkStringStr(c.Name)
			writeCommandDoc(enc, c)
		}
		return
	}

	names := ctx.Argv[2:]
	var found []*CmdDesc
	for _, n := range names {
		if c := ctx.d.table.commandLookupForInfo(string(n)); c != nil {
			found = append(found, c)
		}
	}
	enc.WriteMapLen(len(found))
	for _, c := range found {
		enc.WriteBulkStringStr(c.Name)
		writeCommandDoc(enc, c)
	}
}

// writeCommandDoc writes the documentation map for one command. since and group
// come from the descriptor and are always present; summary, complexity, and
// arguments come from the overlay when it has them. Container commands carry a
// subcommands map.
func writeCommandDoc(enc *resp.Encoder, cmd *CmdDesc) {
	doc := docFor(cmd)

	fields := 2 // summary and since are always written (summary may be derived)
	if g := groupName[cmd.Group]; g != "" {
		fields++
	}
	if doc.complexity != "" {
		fields++
	}
	fields++ // arguments, always written even when empty
	if len(cmd.SubCmds) > 0 {
		fields++
	}

	enc.WriteMapLen(fields)

	enc.WriteBulkStringStr("summary")
	enc.WriteBulkStringStr(summaryFor(cmd, doc))

	enc.WriteBulkStringStr("since")
	since := cmd.Since
	if since == "" {
		since = "1.0.0"
	}
	enc.WriteBulkStringStr(since)

	if g := groupName[cmd.Group]; g != "" {
		enc.WriteBulkStringStr("group")
		enc.WriteBulkStringStr(g)
	}

	if doc.complexity != "" {
		enc.WriteBulkStringStr("complexity")
		enc.WriteBulkStringStr(doc.complexity)
	}

	enc.WriteBulkStringStr("arguments")
	writeDocArgs(enc, doc.args)

	if len(cmd.SubCmds) > 0 {
		enc.WriteBulkStringStr("subcommands")
		enc.WriteMapLen(len(cmd.SubCmds))
		for _, sub := range cmd.SubCmds {
			enc.WriteBulkStringStr(cmd.Name + "|" + sub.Name)
			writeCommandDoc(enc, sub)
		}
	}
}

// summaryFor returns the command's summary, falling back to a readable line built
// from the name when the overlay has none, so every command answers with a
// summary the way Redis does.
func summaryFor(cmd *CmdDesc, doc cmdDoc) string {
	if doc.summary != "" {
		return doc.summary
	}
	name := cmd.Name
	if cmd.SubName != "" {
		name = strings.ReplaceAll(cmd.SubName, "|", " ")
	}
	return "The " + strings.ToUpper(name) + " command."
}

// writeDocArgs writes an array of structured argument maps.
func writeDocArgs(enc *resp.Encoder, args []docArg) {
	enc.WriteArrayLen(len(args))
	for _, a := range args {
		writeDocArg(enc, a)
	}
}

// writeDocArg writes one argument map. Only the fields that are set are written,
// matching the variable shape Redis uses per argument type.
func writeDocArg(enc *resp.Encoder, a docArg) {
	fields := 2 // name and type are always present
	if a.displayText != "" {
		fields++
	}
	if a.token != "" {
		fields++
	}
	if len(a.flags) > 0 {
		fields++
	}
	if len(a.args) > 0 {
		fields++
	}

	enc.WriteMapLen(fields)

	enc.WriteBulkStringStr("name")
	enc.WriteBulkStringStr(a.name)
	enc.WriteBulkStringStr("type")
	enc.WriteBulkStringStr(a.typ)

	if a.displayText != "" {
		enc.WriteBulkStringStr("display_text")
		enc.WriteBulkStringStr(a.displayText)
	}
	if a.token != "" {
		enc.WriteBulkStringStr("token")
		enc.WriteBulkStringStr(a.token)
	}
	if len(a.flags) > 0 {
		enc.WriteBulkStringStr("flags")
		enc.WriteArrayLen(len(a.flags))
		for _, f := range a.flags {
			enc.WriteStatus(f)
		}
	}
	if len(a.args) > 0 {
		enc.WriteBulkStringStr("arguments")
		writeDocArgs(enc, a.args)
	}
}
