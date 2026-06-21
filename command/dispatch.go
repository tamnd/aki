package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// Config holds the server settings the dispatcher needs.
type Config struct {
	// Databases is the number of logical databases. SELECT accepts 0..Databases-1.
	Databases int
	// RequirePass is the default user password. Empty means no auth is required.
	RequirePass string
	// Version is reported by HELLO.
	Version string
	// Mode is reported by HELLO: "standalone", "sentinel", or "cluster".
	Mode string
	// Engine is the keyspace the data commands operate on. It may be nil for a
	// connection-only server (the connection-group commands need no keyspace).
	Engine *Engine
}

// Dispatcher routes parsed commands to their handlers. It satisfies
// networking.Handler.
type Dispatcher struct {
	table  *Table
	cfg    Config
	engine *Engine
	ps     *pubsubRegistry
	conf   *configStore
}

// New builds a Dispatcher with the connection-group and data-type commands.
func New(cfg Config) *Dispatcher {
	if cfg.Databases <= 0 {
		cfg.Databases = 16
	}
	if cfg.Version == "" {
		cfg.Version = "7.2.0-aki-0.1.0"
	}
	if cfg.Mode == "" {
		cfg.Mode = "standalone"
	}
	cmds := connectionCommands()
	cmds = append(cmds, stringCommands()...)
	cmds = append(cmds, bitmapCommands()...)
	cmds = append(cmds, bitfieldCommands()...)
	cmds = append(cmds, listCommands()...)
	cmds = append(cmds, listModifyCommands()...)
	cmds = append(cmds, listMultiCommands()...)
	cmds = append(cmds, hashCommands()...)
	cmds = append(cmds, hashExtraCommands()...)
	cmds = append(cmds, hashTTLCommands()...)
	cmds = append(cmds, hashGetExCommands()...)
	cmds = append(cmds, setCommands()...)
	cmds = append(cmds, setAlgebraCommands()...)
	cmds = append(cmds, zsetCommands()...)
	cmds = append(cmds, zsetRankCommands()...)
	cmds = append(cmds, zsetRangeCommands()...)
	cmds = append(cmds, zsetOpCommands()...)
	cmds = append(cmds, hllCommands()...)
	cmds = append(cmds, geoCommands()...)
	cmds = append(cmds, streamCommands()...)
	cmds = append(cmds, claimCommands()...)
	cmds = append(cmds, expireCommands()...)
	cmds = append(cmds, scanCommands()...)
	cmds = append(cmds, aggScanCommands()...)
	cmds = append(cmds, keyopsCommands()...)
	cmds = append(cmds, adminCommands()...)
	cmds = append(cmds, objectCommands()...)
	cmds = append(cmds, transactionCommands()...)
	cmds = append(cmds, pubsubCommands()...)
	cmds = append(cmds, configCommands()...)
	cmds = append(cmds, genericCommands()...)
	conf := newConfigStore()
	conf.set("databases", strconv.Itoa(cfg.Databases))
	if cfg.RequirePass != "" {
		conf.set("requirepass", cfg.RequirePass)
	}
	return &Dispatcher{table: NewTable(cmds), cfg: cfg, engine: cfg.Engine, ps: newPubsubRegistry(), conf: conf}
}

// Ctx carries everything a handler needs: the connection it replies on, the
// argument vector, and back-references to the dispatcher and session. Later
// milestones add keyspace accessors here.
type Ctx struct {
	Conn *networking.Conn
	Argv [][]byte
	d    *Dispatcher
	sess *session
}

// session is the command-layer per-connection state stored in the opaque slot
// on networking.Conn.
type session struct {
	authenticated bool

	// inMulti is true between MULTI and EXEC/DISCARD: commands are queued instead
	// of run. queue holds them in order, and dirtyExec records a queue-time error
	// (unknown command or bad arity) that makes EXEC abort.
	inMulti   bool
	queue     []queuedCmd
	dirtyExec bool
	// watched holds the keys registered by WATCH with their version at WATCH time.
	// EXEC compares against the current versions to decide whether to run.
	watched []watchEntry

	// Pub/Sub subscriptions held by this connection, one set per namespace. The
	// running total across the three is the subscriber-mode count.
	subChannels map[string]bool
	subPatterns map[string]bool
	subShards   map[string]bool
}

// subCount is the running number of subscriptions across channels, patterns and
// shard channels. A RESP2 client with a non-zero count is in subscriber mode.
func (s *session) subCount() int {
	return len(s.subChannels) + len(s.subPatterns) + len(s.subShards)
}

// enc returns the connection's reply encoder.
func (ctx *Ctx) enc() *resp.Encoder { return ctx.Conn.Enc() }

// Handle implements networking.Handler. It runs the dispatch pipeline: look up
// the command, check arity, check auth, then call the handler.
func (d *Dispatcher) Handle(c *networking.Conn, argv [][]byte) {
	sess := d.sessionFor(c)
	name := strings.ToLower(string(argv[0]))

	// A RESP2 client with active subscriptions is in subscriber mode and may run
	// only the subscribe family plus PING, QUIT and RESET. RESP3 lifts this.
	if c.Proto() == 2 && sess.subCount() > 0 && !allowedInSubMode(name) {
		c.Enc().WriteError("ERR Command not allowed inside a subscription context. Please use RESET.")
		return
	}

	// Inside a transaction every command except the control verbs is queued
	// rather than run. EXEC drains the queue later.
	if sess.inMulti && !isMultiControl(name) {
		d.queueCommand(c, sess, name, argv)
		return
	}

	cmd, err := d.table.lookup(name, argv)
	if err != nil {
		c.Enc().WriteError(err.Error())
		return
	}
	if !checkArity(cmd, len(argv)) {
		c.Enc().WriteError(arityError(cmd))
		return
	}
	if d.cfg.RequirePass != "" && !sess.authenticated && !cmd.Flags.Has(FlagNoAuth) {
		c.Enc().WriteError("NOAUTH Authentication required.")
		return
	}

	cmd.Handler(&Ctx{Conn: c, Argv: argv, d: d, sess: sess})
}

// sessionFor returns the connection's session, creating it on first use. A new
// connection starts authenticated only when no password is configured.
func (d *Dispatcher) sessionFor(c *networking.Conn) *session {
	if s, ok := c.Session().(*session); ok {
		return s
	}
	s := &session{authenticated: d.cfg.RequirePass == ""}
	c.SetSession(s)
	return s
}

// OnDisconnect drops a connection's pub/sub subscriptions when its read loop
// exits, so a published message is never delivered to a gone client. It satisfies
// networking.DisconnectHandler.
func (d *Dispatcher) OnDisconnect(c *networking.Conn) {
	sess, ok := c.Session().(*session)
	if !ok {
		return
	}
	d.ps.dropClient(c.ID(), sess)
}
