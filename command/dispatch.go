package command

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// Config holds the server settings the dispatcher needs.
type Config struct {
	// Databases is the number of logical databases. SELECT accepts 0..Databases-1.
	Databases int
	// RequirePass is the default user password. Empty means no auth is required.
	RequirePass string
	// AclFile is the path to an external ACL file. Empty disables ACL LOAD/SAVE
	// and keeps users in memory only.
	AclFile string
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
	acl    *aclRegistry
	srv    *networking.Server

	// startTime is when the dispatcher was built, used for INFO uptime. runID is
	// a 40-hex random identifier generated once at startup, reported by INFO as
	// run_id and reused as the replication id.
	startTime time.Time
	runID     string

	// notifyFlags is the parsed notify-keyspace-events bitmask. The write path
	// reads it with an atomic load so a server with notifications off pays almost
	// nothing; CONFIG SET updates it with an atomic store.
	notifyFlags uint32

	// hz is the background tick rate, the number of active expiry cycles per
	// second. activeExpire gates that cycle: DEBUG SET-ACTIVE-EXPIRE 0 clears it
	// so only lazy expiry runs, which tests rely on. bgStop and bgDone manage the
	// background goroutine started by StartBackground.
	hz           int
	activeExpire atomic.Bool
	bgStop       chan struct{}
	bgDone       chan struct{}

	// persist holds the RDB save bookkeeping that SAVE, BGSAVE, LASTSAVE, the
	// automatic save points, and INFO persistence all read and write.
	persist persistState

	// aof holds the append-only-file emulation state that BGREWRITEAOF, the
	// auto-rewrite trigger, and INFO persistence read and write.
	aof aofState

	// scripts is the EVAL/EVALSHA script cache, keyed by lowercase SHA1 hex of the
	// body. SCRIPT LOAD fills it, EVALSHA reads it, SCRIPT FLUSH clears it.
	scripts scriptCache

	// functions holds the FUNCTION LOAD libraries and FCALL targets.
	functions functionRegistry

	// repl holds the replication state: the master-side backlog and replica list,
	// and the replica-side link to a master.
	repl replState

	// cluster holds the cluster topology: this node's id, epoch, and the slots it
	// owns. aki runs single-node by default, so this stays mostly empty unless
	// cluster-enabled is on and slots are assigned.
	cluster clusterState

	// tracking holds the client-side caching state: the per-key invalidation table
	// for default-mode tracking and the prefix table for BCAST mode. It carries its
	// own lock and an atomic client counter so the write path can skip all tracking
	// work with a single load when no client is tracking.
	tracking trackingState

	// stats holds the per-command call, time, and error counters behind the INFO
	// commandstats, latencystats, and errorstats sections and CONFIG RESETSTAT.
	stats statsState

	// slowlog holds the ring of recent slow commands behind the SLOWLOG command.
	slowlog slowlogState

	// latency holds the per-event latency spike histories behind the LATENCY
	// command. It is separate from the per-command histograms in stats.
	latency latencyState

	// metrics holds the running Prometheus endpoint, started when metrics-port is
	// set and shut down on server stop.
	metrics metricsServer
}

// SetServer gives the dispatcher a handle to the network server so CLIENT and
// INFO can enumerate live connections. The wiring happens after both are built,
// since the server takes the dispatcher as its handler.
func (d *Dispatcher) SetServer(s *networking.Server) { d.srv = s }

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
	cmds = append(cmds, sortCommands()...)
	cmds = append(cmds, dumpCommands()...)
	cmds = append(cmds, migrateCommands()...)
	cmds = append(cmds, persistenceCommands()...)
	cmds = append(cmds, aofCommands()...)
	cmds = append(cmds, adminCommands()...)
	cmds = append(cmds, objectCommands()...)
	cmds = append(cmds, transactionCommands()...)
	cmds = append(cmds, pubsubCommands()...)
	cmds = append(cmds, configCommands()...)
	cmds = append(cmds, aclCommands()...)
	cmds = append(cmds, clientCommands()...)
	cmds = append(cmds, infoCommands()...)
	cmds = append(cmds, debugCommands()...)
	cmds = append(cmds, slowlogCommands()...)
	cmds = append(cmds, latencyCommands()...)
	cmds = append(cmds, memoryCommands()...)
	cmds = append(cmds, scriptCommands()...)
	cmds = append(cmds, functionCommands()...)
	cmds = append(cmds, replicationCommands()...)
	cmds = append(cmds, clusterCommands()...)
	cmds = append(cmds, sentinelCommands()...)
	cmds = append(cmds, genericCommands()...)
	conf := newConfigStore()
	conf.set("databases", strconv.Itoa(cfg.Databases))
	if cfg.RequirePass != "" {
		conf.set("requirepass", cfg.RequirePass)
	}
	acl := newACLRegistry(cfg.RequirePass)
	acl.aclFile = cfg.AclFile
	d := &Dispatcher{
		table:     NewTable(cmds),
		cfg:       cfg,
		engine:    cfg.Engine,
		ps:        newPubsubRegistry(),
		conf:      conf,
		acl:       acl,
		startTime: time.Now(),
		runID:     newRunID(),
		hz:        10,
	}
	d.activeExpire.Store(true)
	d.replInit()
	d.clusterInit()
	d.trackingInit()
	d.statsInit()
	d.latencyInit()
	if cfg.AclFile != "" {
		// A missing or unreadable file at startup is not fatal: the in-memory
		// default user stays in place until ACL LOAD or ACL SAVE is run.
		_ = acl.loadFile()
	}
	if v, ok := conf.get("notify-keyspace-events"); ok {
		if flags, ok := parseNotifyFlags(v); ok {
			d.notifyFlags = flags
		}
	}
	return d
}

// StartBackground launches the server cron, a goroutine that runs the active
// expiry cycle hz times a second. The network server calls it once at startup.
// Tests that drive expiry directly do not start it; they call runActiveExpire
// instead so the timing is deterministic.
func (d *Dispatcher) StartBackground() {
	if d.bgStop != nil {
		return
	}
	d.initAOF()
	d.bgStop = make(chan struct{})
	d.bgDone = make(chan struct{})
	interval := time.Second / time.Duration(d.hz)
	go func() {
		defer close(d.bgDone)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-d.bgStop:
				return
			case <-t.C:
				d.runActiveExpire()
				d.checkSavePoints()
				d.checkAOFRewrite()
				d.replPingReplicas()
			}
		}
	}()
}

// StopBackground stops the cron goroutine and waits for it to exit. It is safe to
// call when the cron was never started.
func (d *Dispatcher) StopBackground() {
	if d.bgStop == nil {
		return
	}
	close(d.bgStop)
	<-d.bgDone
	d.bgStop = nil
	d.bgDone = nil
	d.closeAOF()
}

// runActiveExpire runs one active expiry pass and fires the expired event for
// every key it removed. It is a no-op when active expiry is disabled or no
// keyspace is attached. The cron loop and the tests both call it.
func (d *Dispatcher) runActiveExpire() {
	if d.engine == nil || !d.activeExpire.Load() {
		return
	}
	if err := d.engine.activeExpireCycle(); err != nil {
		return
	}
	d.drainExpired()
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

	// user is the ACL user this connection runs as, and username is its name.
	// A fresh connection starts as the default user; AUTH changes both.
	user     *aclUser
	username string

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

	// CLIENT introspection state. lastCmd is the most recent command name (with
	// its subcommand for container commands), libName and libVer come from CLIENT
	// SETINFO, and noEvict and noTouch hold the per-connection CLIENT toggles.
	lastCmd string
	libName string
	libVer  string
	noEvict bool
	noTouch bool

	// Replication. isReplica marks a connection that issued PSYNC/SYNC and is now
	// a downstream replica. replListenPort is the port it announced with REPLCONF
	// listening-port. fromMaster marks the internal connection the replica apply
	// loop uses, so its writes bypass the read-only guard and are not re-propagated.
	isReplica      bool
	replListenPort int
	fromMaster     bool

	// Client-side caching (CLIENT TRACKING). trackingOn marks a connection that
	// asked the server to record the keys it reads and push invalidations when
	// they change. The mode flags are mutually exclusive in the ways CLIENT
	// TRACKING enforces: bcast tracks by prefix instead of by read key, optIn and
	// optOut flip whether a command is tracked by default. trackingPrefixes holds
	// the BCAST prefixes, trackingRedir is the client id RESP2 invalidations are
	// forwarded to (0 when the client takes them inline over RESP3). cachingYes and
	// cachingNo carry a one-shot CLIENT CACHING decision for the very next command.
	trackingOn       bool
	trackingBcast    bool
	trackingOptIn    bool
	trackingOptOut   bool
	trackingNoLoop   bool
	trackingPrefixes []string
	trackingRedir    uint64
	cachingYes       bool
	cachingNo        bool
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
		msg := "ERR Command not allowed inside a subscription context. Please use RESET."
		c.Enc().WriteError(msg)
		d.statError(msg)
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
		d.statError(err.Error())
		return
	}
	if cmd.SubName != "" {
		sess.lastCmd = cmd.SubName
	} else {
		sess.lastCmd = name
	}
	if !checkArity(cmd, len(argv)) {
		msg := arityError(cmd)
		c.Enc().WriteError(msg)
		d.statReject(cmd)
		d.statError(msg)
		return
	}
	if msg := d.aclEnforce(c, sess, cmd, argv); msg != "" {
		c.Enc().WriteError(msg)
		d.statReject(cmd)
		d.statError(msg)
		return
	}
	if cmd.Flags.Has(FlagWrite) && !sess.fromMaster && d.isReadonlyReplica() {
		msg := "READONLY You can't write against a read only replica."
		c.Enc().WriteError(msg)
		d.statReject(cmd)
		d.statError(msg)
		return
	}

	d.runCommand(&Ctx{Conn: c, Argv: argv, d: d, sess: sess}, cmd)
}

// sessionFor returns the connection's session, creating it on first use. A new
// connection starts authenticated only when no password is configured.
func (d *Dispatcher) sessionFor(c *networking.Conn) *session {
	if s, ok := c.Session().(*session); ok {
		return s
	}
	def := d.acl.get("default")
	s := &session{
		authenticated: def != nil && def.nopass,
		user:          def,
		username:      "default",
	}
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
	if sess.isReplica {
		d.dropReplica(c.ID())
	}
	if sess.trackingOn {
		d.trackingDropClient(c.ID(), sess)
	}
	d.ps.dropClient(c.ID(), sess)
}
