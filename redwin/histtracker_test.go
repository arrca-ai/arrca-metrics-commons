// SPDX-License-Identifier: Apache-2.0
package redwin

import (
	"testing"
	"time"
)

func TestHistTrackerFirstSampleNoDelta(t *testing.T) {
	tr := NewHistTracker(8)
	_, _, _, ok := tr.Delta("k", 10, []uint64{1, 2, 3}, 1_000_000_000, time.Unix(0, 0))
	if ok {
		t.Fatal("first sample must not yield a delta")
	}
}

func TestHistTrackerDelta(t *testing.T) {
	tr := NewHistTracker(8)
	now := time.Unix(100, 0)
	tr.Delta("k", 10, []uint64{1, 2, 3}, 1_000_000_000, now)                                 // baseline at t=1s
	dCount, dBuckets, dtSec, ok := tr.Delta("k", 25, []uint64{2, 5, 8}, 16_000_000_000, now) // t=16s
	if !ok {
		t.Fatal("second sample must yield a delta")
	}
	if dCount != 15 {
		t.Fatalf("dCount=%d want 15", dCount)
	}
	if dBuckets[0] != 1 || dBuckets[1] != 3 || dBuckets[2] != 5 {
		t.Fatalf("dBuckets=%v want [1 3 5]", dBuckets)
	}
	if dtSec != 15 {
		t.Fatalf("dtSec=%v want 15", dtSec)
	}
}

func TestHistTrackerResetDetected(t *testing.T) {
	tr := NewHistTracker(8)
	now := time.Unix(100, 0)
	tr.Delta("k", 100, []uint64{50}, 1_000_000_000, now)
	if _, _, _, ok := tr.Delta("k", 5, []uint64{2}, 2_000_000_000, now); ok {
		t.Fatal("counter reset must not yield a delta")
	}
}

func TestHistTrackerBucketDecrease(t *testing.T) {
	// Count stays the same but one bucket decreases — this must trigger the
	// per-bucket reset path (ok=false), independent of the counter-decrease path.
	tr := NewHistTracker(8)
	now := time.Unix(100, 0)
	// sample1: count=10, buckets=[5,5]
	tr.Delta("k", 10, []uint64{5, 5}, 1_000_000_000, now)
	// sample2: count=10 (unchanged), bucket[1] decreases 5→4 → per-bucket reset.
	if _, _, _, ok := tr.Delta("k", 10, []uint64{6, 4}, 2_000_000_000, now); ok {
		t.Fatal("bucket decrease while count unchanged must return ok=false")
	}
}

func TestHistTrackerReap(t *testing.T) {
	tr := NewHistTracker(8)
	base := time.Unix(100, 0)
	tr.Delta("k", 1, []uint64{1}, 1_000_000_000, base)
	if n := tr.Reap(base.Add(20*time.Minute), 10*time.Minute); n != 1 {
		t.Fatalf("reaped %d want 1", n)
	}
	// after reaping, the series is a fresh baseline again
	if _, _, _, ok := tr.Delta("k", 2, []uint64{2}, 2_000_000_000, base.Add(20*time.Minute)); ok {
		t.Fatal("reaped series must behave as first sample")
	}
}
