package command

import (
	"sort"
	"sync"
	"time"
)

// This file implements the latency monitor from doc 20 section 4: per-event
// histories of latency spikes above latency-monitor-threshold. It is separate
// from the per-command latency histograms in stats.go, which back the INFO
// latencystats section. The monitor records discrete spikes by event class and
// drives LATENCY HISTORY, LATEST, RESET, DOCTOR, and GRAPH.

// latencyMaxSamples caps the per-event history. Redis keeps 160; aki keeps 180 so
// the GRAPH sparkline has a full line to draw.
const latencyMaxSamples = 180

// latencySample is one recorded spike: when it happened and how long it took.
type latencySample struct {
	ts int64
	ms int64
}

// latencyHistory is the rolling spike history for one event class, plus the
// running latest and max used by LATENCY LATEST.
type latencyHistory struct {
	samples  []latencySample
	latestMs int64
	latestTS int64
	maxMs    int64
}

// latencyState holds every event history behind one lock.
type latencyState struct {
	mu     sync.Mutex
	events map[string]*latencyHistory
}

// latencyInit allocates the event map. New calls it once at startup.
func (d *Dispatcher) latencyInit() {
	d.latency.events = make(map[string]*latencyHistory)
}

// latencyEventFor names the latency event class for a finished command. Commands
// flagged fast (the O(1) family like GET, SET, HGET) record under fast-command;
// everything else records under command, matching how Redis splits the two.
func latencyEventFor(cmd *CmdDesc) string {
	if cmd.Flags.Has(FlagFast) {
		return "fast-command"
	}
	return "command"
}

// latencyAddSample records a spike for an event when its duration is at or above
// latency-monitor-threshold. A threshold of 0 disables the monitor. ms is the
// event duration in milliseconds.
func (d *Dispatcher) latencyAddSample(event string, ms int64) {
	threshold := d.confInt("latency-monitor-threshold", 0)
	if threshold <= 0 || ms < threshold {
		return
	}
	now := time.Now().Unix()
	d.latency.mu.Lock()
	h := d.latency.events[event]
	if h == nil {
		h = &latencyHistory{}
		d.latency.events[event] = h
	}
	h.samples = append(h.samples, latencySample{ts: now, ms: ms})
	if len(h.samples) > latencyMaxSamples {
		h.samples = h.samples[len(h.samples)-latencyMaxSamples:]
	}
	if ms > h.maxMs {
		h.maxMs = ms
	}
	h.latestMs = ms
	h.latestTS = now
	d.latency.mu.Unlock()
}

// latencyHistoryOf returns a copy of the samples for an event, oldest first, or
// nil when the event has none.
func (d *Dispatcher) latencyHistoryOf(event string) []latencySample {
	d.latency.mu.Lock()
	defer d.latency.mu.Unlock()
	h := d.latency.events[event]
	if h == nil {
		return nil
	}
	out := make([]latencySample, len(h.samples))
	copy(out, h.samples)
	return out
}

// latencyLatestEntry is one row of LATENCY LATEST.
type latencyLatestEntry struct {
	event    string
	latestTS int64
	latestMs int64
	maxMs    int64
}

// latencyLatest returns the latest and max spike for every event with samples,
// sorted by event name so the output is stable.
func (d *Dispatcher) latencyLatest() []latencyLatestEntry {
	d.latency.mu.Lock()
	defer d.latency.mu.Unlock()
	out := make([]latencyLatestEntry, 0, len(d.latency.events))
	for name, h := range d.latency.events {
		if len(h.samples) == 0 {
			continue
		}
		out = append(out, latencyLatestEntry{
			event:    name,
			latestTS: h.latestTS,
			latestMs: h.latestMs,
			maxMs:    h.maxMs,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].event < out[j].event })
	return out
}

// latencyReset clears the named events, or every event when names is empty, and
// returns how many histories were dropped.
func (d *Dispatcher) latencyReset(names []string) int {
	d.latency.mu.Lock()
	defer d.latency.mu.Unlock()
	if len(names) == 0 {
		n := len(d.latency.events)
		d.latency.events = make(map[string]*latencyHistory)
		return n
	}
	n := 0
	for _, name := range names {
		if _, ok := d.latency.events[name]; ok {
			delete(d.latency.events, name)
			n++
		}
	}
	return n
}
