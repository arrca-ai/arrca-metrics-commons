// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"sync"
	"time"
)

type rateState struct {
	lastValue  float64
	lastUnixMs int64
	updatedAt  time.Time
}

// RateTracker converts cumulative counters to per-second rates, keeping the last
// sample per series key in memory. Safe for concurrent use.
type RateTracker struct {
	mu  sync.Mutex
	m   map[string]rateState
	ttl time.Duration
}

func NewRateTracker(ttl time.Duration) *RateTracker {
	return &RateTracker{m: make(map[string]rateState), ttl: ttl}
}

// Rate records a cumulative sample and returns the rate since the previous sample
// for key. ok=false on first sample, counter reset (value<prev), or dt<=0; the
// caller emits nothing in those cases. The new sample always becomes the baseline.
func (r *RateTracker) Rate(key string, value float64, tUnixMs int64, now time.Time) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, seen := r.m[key]
	r.m[key] = rateState{lastValue: value, lastUnixMs: tUnixMs, updatedAt: now}
	if !seen || value < prev.lastValue {
		return 0, false
	}
	dtMs := tUnixMs - prev.lastUnixMs
	if dtMs <= 0 {
		return 0, false
	}
	return (value - prev.lastValue) / (float64(dtMs) / 1000.0), true
}

// Reap drops state not updated within ttl and returns the count removed.
func (r *RateTracker) Reap(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Add(-r.ttl)
	n := 0
	for k, st := range r.m {
		if st.updatedAt.Before(cutoff) {
			delete(r.m, k)
			n++
		}
	}
	return n
}
