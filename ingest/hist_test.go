// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"math"
	"testing"
	"time"
)

func approx(a, b float64) bool { return math.Abs(a-b) < 0.01 }

func TestHistTracker_Percentiles(t *testing.T) {
	h := NewHistTracker(time.Minute)
	bounds := []float64{10, 50, 100, 500}
	now := time.UnixMilli(0)

	// First sample: baseline only, no result.
	if _, ok := h.Percentiles("k", bounds, []uint64{10, 5, 2, 1, 0}, []float64{0.5, 0.9, 0.99}, now); ok {
		t.Fatal("first sample must return ok=false")
	}
	// Second sample: window diff = [60,25,10,3,2], total 100.
	vals, ok := h.Percentiles("k", bounds, []uint64{70, 30, 12, 4, 2}, []float64{0.5, 0.9, 0.99}, now)
	if !ok {
		t.Fatal("second sample should produce percentiles")
	}
	// p50 rank 50 → bucket0 [0,10], frac 50/60 → 8.33; p90 rank 90 → bucket2 (50,100], frac 5/10 → 75;
	// p99 rank 99 → overflow bucket → clamp to max finite bound 500.
	if !approx(vals[0], 8.333) || !approx(vals[1], 75) || vals[2] != 500 {
		t.Fatalf("percentiles wrong: %+v (want ~8.33, 75, 500)", vals)
	}
}

func TestHistTracker_Guards(t *testing.T) {
	h := NewHistTracker(time.Minute)
	bounds := []float64{10, 50}
	now := time.UnixMilli(0)
	h.Percentiles("k", bounds, []uint64{5, 5, 5}, []float64{0.5}, now) // prime

	// Empty window (identical cumulative) → ok=false.
	if _, ok := h.Percentiles("k", bounds, []uint64{5, 5, 5}, []float64{0.5}, now); ok {
		t.Fatal("empty window must return ok=false")
	}
	// Counter reset (a bucket drops) → ok=false.
	if _, ok := h.Percentiles("k", bounds, []uint64{1, 1, 1}, []float64{0.5}, now); ok {
		t.Fatal("reset must return ok=false")
	}
	// Bounds change → ok=false (after reset re-primed above with [1,1,1]).
	if _, ok := h.Percentiles("k", []float64{10, 50, 100}, []uint64{2, 2, 2, 2}, []float64{0.5}, now); ok {
		t.Fatal("bounds change must return ok=false")
	}
	// Degenerate: counts length != bounds+1 → ok=false.
	if _, ok := h.Percentiles("d", []float64{10, 50}, []uint64{1, 1}, []float64{0.5}, now); ok {
		t.Fatal("mismatched counts length must return ok=false")
	}
	// Degenerate: no explicit bounds → ok=false.
	if _, ok := h.Percentiles("e", []float64{}, []uint64{5}, []float64{0.5}, now); ok {
		t.Fatal("no bounds must return ok=false")
	}
}

func TestHistTracker_Reap(t *testing.T) {
	h := NewHistTracker(time.Minute)
	bounds := []float64{10, 50}
	h.Percentiles("k", bounds, []uint64{1, 1, 1}, []float64{0.5}, time.UnixMilli(0))
	if n := h.Reap(time.UnixMilli(0).Add(2 * time.Minute)); n != 1 {
		t.Fatalf("want 1 reaped, got %d", n)
	}
}
