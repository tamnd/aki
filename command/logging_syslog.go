//go:build !windows

package command

import (
	"log/syslog"
	"strings"
)

// This file holds the syslog sink for the unix builds, where log/syslog is
// available. The windows build gets a stub in logging_nosyslog.go. The
// platform-neutral logger in logging.go uses the syslogSink type and the
// newSyslogSink constructor that both files provide.

// syslogSink wraps the optional syslog destination and routes each level to the
// matching syslog priority.
type syslogSink struct {
	w *syslog.Writer
}

// newSyslogSink connects to the local syslog daemon with the given program ident
// and facility. An unknown facility name falls back to LOG_LOCAL0, the spec
// default.
func newSyslogSink(ident, facility string) (*syslogSink, error) {
	w, err := syslog.New(syslogFacility(facility), ident)
	if err != nil {
		return nil, err
	}
	return &syslogSink{w: w}, nil
}

// write sends the message at the syslog priority that matches the aki level.
func (s *syslogSink) write(level int, msg string) {
	switch level {
	case logDebug:
		_ = s.w.Debug(msg)
	case logVerbose:
		_ = s.w.Info(msg)
	case logWarning:
		_ = s.w.Warning(msg)
	default:
		_ = s.w.Notice(msg)
	}
}

// Close closes the syslog connection.
func (s *syslogSink) Close() error { return s.w.Close() }

// syslogFacility maps a facility name to its log/syslog priority bits.
func syslogFacility(name string) syslog.Priority {
	switch strings.ToLower(name) {
	case "user":
		return syslog.LOG_USER
	case "daemon":
		return syslog.LOG_DAEMON
	case "local0":
		return syslog.LOG_LOCAL0
	case "local1":
		return syslog.LOG_LOCAL1
	case "local2":
		return syslog.LOG_LOCAL2
	case "local3":
		return syslog.LOG_LOCAL3
	case "local4":
		return syslog.LOG_LOCAL4
	case "local5":
		return syslog.LOG_LOCAL5
	case "local6":
		return syslog.LOG_LOCAL6
	case "local7":
		return syslog.LOG_LOCAL7
	default:
		return syslog.LOG_LOCAL0
	}
}
