// Package command implements aki's command table and dispatch pipeline. It
// turns a parsed argument vector into a reply by looking the command up in the
// table, checking arity and auth, and calling the command's handler. It is the
// Handler the networking layer drives (doc 07).
//
// This milestone (M1) builds the dispatch core and the connection-group
// commands. The data-type commands (strings, lists, ...) register into the same
// table in later slices; the keyspace they touch is reached through the handler
// context, which gains those accessors when the keyspace lands.
package command

// CmdGroup is the functional group a command belongs to, used by COMMAND and by
// ACL categorisation (doc 07 §1.1).
type CmdGroup uint8

const (
	GroupString CmdGroup = iota
	GroupBitmap
	GroupHyperLogLog
	GroupGeo
	GroupList
	GroupSet
	GroupSortedSet
	GroupHash
	GroupStream
	GroupPubSub
	GroupTransactions
	GroupScripting
	GroupGeneric
	GroupServer
	GroupConnection
	GroupCluster
)

// FlagSet is a bitmask of command flags (doc 07 §1.2, §2). Only the flags the
// current milestone observes are acted on; the rest are carried for COMMAND
// introspection and future pipeline stages.
type FlagSet uint64

const (
	FlagWrite FlagSet = 1 << iota
	FlagReadOnly
	FlagDenyOOM
	FlagAdmin
	FlagPubSub
	FlagNoScript
	FlagBlocking
	FlagLoading
	FlagStale
	FlagNoAuth
	FlagFast
	FlagMovableKeys
	FlagNoMulti
	FlagNoMandatoryKeys
	FlagAllowBusy
)

// Has reports whether every bit in f is set in s.
func (s FlagSet) Has(f FlagSet) bool { return s&f == f }

// CmdDesc describes one command in the table: its identity, arity, flags, key
// positions, and handler. It is the single source of truth for dispatch, arity
// checking, and introspection (doc 07 §1).
type CmdDesc struct {
	Name    string // lowercase, e.g. "get", "hello"
	SubName string // "container|sub" for subcommands, e.g. "command|count"
	Group   CmdGroup
	Since   string // Redis version the command first appeared in

	// Arity follows the Redis convention and includes the command name itself:
	// a positive value is an exact count, a negative value is a minimum of its
	// absolute value.
	Arity int

	Flags FlagSet

	// Fixed key positions (1-based). FirstKey 0 means the command takes no keys.
	FirstKey int
	LastKey  int
	Step     int

	// Handler runs the command and writes its reply through ctx.Conn.
	Handler func(ctx *Ctx)

	// SubCmds holds the subcommands of a container command (COMMAND, CONFIG, ...).
	SubCmds []*CmdDesc
}
