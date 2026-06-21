package command

import (
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
	cmds = append(cmds, setCommands()...)
	cmds = append(cmds, setAlgebraCommands()...)
	cmds = append(cmds, zsetCommands()...)
	cmds = append(cmds, zsetRankCommands()...)
	cmds = append(cmds, genericCommands()...)
	return &Dispatcher{table: NewTable(cmds), cfg: cfg, engine: cfg.Engine}
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
}

// enc returns the connection's reply encoder.
func (ctx *Ctx) enc() *resp.Encoder { return ctx.Conn.Enc() }

// Handle implements networking.Handler. It runs the dispatch pipeline: look up
// the command, check arity, check auth, then call the handler.
func (d *Dispatcher) Handle(c *networking.Conn, argv [][]byte) {
	sess := d.sessionFor(c)
	name := strings.ToLower(string(argv[0]))

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
