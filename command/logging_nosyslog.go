//go:build windows

package command

import "errors"

// Windows has no log/syslog, so this stub stands in for the unix sink in
// logging_syslog.go. The type and constructor exist so the platform-neutral
// logger in logging.go compiles, but turning syslog-enabled on returns an error
// instead of silently doing nothing. Default logging to stderr or a file works
// the same as everywhere else.

// syslogSink is the no-op sink used on platforms without syslog.
type syslogSink struct{}

// newSyslogSink always fails on Windows because syslog is a unix facility.
func newSyslogSink(ident, facility string) (*syslogSink, error) {
	return nil, errors.New("syslog is not supported on this platform")
}

// write does nothing; the sink is never constructed on Windows.
func (s *syslogSink) write(level int, msg string) {}

// Close does nothing.
func (s *syslogSink) Close() error { return nil }
