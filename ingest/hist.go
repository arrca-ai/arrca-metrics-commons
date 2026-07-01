// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"sync"
	"time"
)

type histState struct {
	bounds    []float64
	cumCounts []uint64
	updatedAt time.Time
}

// HistTracker converts cumulative explicit-bucket histograms to per-window
// percentiles, keeping the last cumulative bucket vector per series key in
// memory. Safe for concurrent use. Mirrors RateTracker.
type HistTracker struct {
	mu  sync.Mutex
	m   map[string]histState
	ttl time.Duration
}

func NewHistTracker(ttl time.Duration) *HistTracker {
	return &HistTracker{m: make(map[string]histState), ttl: ttl}
}

// Percentiles records the cumulative bucket vector for key and returns the
// per-window percentiles for quantiles (same order). It diffs the new vector
// against the stored previous one to get the window distribution, then
// linear-interpolates each quantile within its bucket. ok=false (caller emits
// nothing; the new sample still becomes the baseline) on: first sample, a
// counter reset (any bucket lower than stored), a bounds change, a degenerate
// histogram (no bounds, or counts length != bounds+1), or an empty window.
func (h *HistTracker) Percentiles(key string, bounds []float64, cumCounts []uint64, quantiles []float64, now time.Time) ([]float64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	prev, seen := h.m[key]
	h.m[key] = histState{bounds: bounds, cumCounts: cumCounts, updatedAt: now}

	if len(bounds) == 0 || len(cumCounts) != len(bounds)+1 {
		return nil, false // degenerate histogram
	}
	if !seen || !equalFloats(prev.bounds, bounds) || len(prev.cumCounts) != len(cumCounts) {
		return nil, false // first sample or bounds change
	}
	window := make([]uint64, len(cumCounts))
	var total uint64
	for i := range cumCounts {
		if cumCounts[i] < prev.cumCounts[i] {
			return nil, false // counter reset
		}
		window[i] = cumCounts[i] - prev.cumCounts[i]
		total += window[i]
	}
	if total == 0 {
		return nil, false // empty window
	}
	out := make([]float64, len(quantiles))
	for qi, q := range quantiles {
		out[qi] = interpolate(bounds, window, total, q)
	}
	return out, true
}

// interpolate returns the q-quantile value of a window distribution over
// explicit buckets. window has len(bounds)+1 entries; bucket i covers
// (bounds[i-1], bounds[i]] with bucket 0's lower bound taken as 0 (values are
// non-negative) and the final overflow bucket clamped to the max finite bound.
func interpolate(bounds []float64, window []uint64, total uint64, q float64) float64 {
	rank := q * float64(total)
	var run float64
	for i := 0; i < len(window); i++ {
		c := float64(window[i])
		if run+c >= rank {
			if i >= len(bounds) { // overflow (+Inf) bucket: no finite upper bound
				return bounds[len(bounds)-1]
			}
			lower := 0.0
			if i > 0 {
				lower = bounds[i-1]
			}
			upper := bounds[i]
			frac := 0.0
			if c > 0 {
				frac = (rank - run) / c
			}
			return lower + frac*(upper-lower)
		}
		run += c
	}
	return bounds[len(bounds)-1] // unreachable when total>0; clamp defensively
}

func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Reap drops state not updated within ttl and returns the count removed.
func (h *HistTracker) Reap(now time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	cutoff := now.Add(-h.ttl)
	n := 0
	for k, st := range h.m {
		if st.updatedAt.Before(cutoff) {
			delete(h.m, k)
			n++
		}
	}
	return n
}
