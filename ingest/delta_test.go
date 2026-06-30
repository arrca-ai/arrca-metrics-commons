// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"testing"
	"time"
)

func TestDeltaTracker_FirstSampleSeedsOnly(t *testing.T) {
	dt := NewDeltaTracker(4)
	now := time.Unix(100, 0)
	if _, ok := dt.Delta("k", 10, now); ok {
		t.Fatal("first sample must return ok=false")
	}
}

func TestDeltaTracker_DeltaVsPrevious(t *testing.T) {
	dt := NewDeltaTracker(4)
	now := time.Unix(100, 0)
	dt.Delta("k", 10, now)
	d, ok := dt.Delta("k", 25, now.Add(time.Second))
	if !ok || d != 15 {
		t.Fatalf("want (15,true), got (%v,%v)", d, ok)
	}
}

func TestDeltaTracker_ResetDropsAndRebaselines(t *testing.T) {
	dt := NewDeltaTracker(4)
	now := time.Unix(100, 0)
	dt.Delta("k", 100, now)
	if _, ok := dt.Delta("k", 5, now.Add(time.Second)); ok {
		t.Fatal("reset (value<last) must return ok=false")
	}
	// New baseline is 5: next delta is measured from 5.
	d, ok := dt.Delta("k", 8, now.Add(2*time.Second))
	if !ok || d != 3 {
		t.Fatalf("want (3,true) after rebaseline, got (%v,%v)", d, ok)
	}
}

func TestDeltaTracker_ReapDropsIdle(t *testing.T) {
	dt := NewDeltaTracker(4)
	base := time.Unix(100, 0)
	dt.Delta("k", 10, base)
	n := dt.Reap(base.Add(10*time.Minute), time.Minute)
	if n != 1 {
		t.Fatalf("want 1 reaped, got %d", n)
	}
	// After reap, the key is gone: next sample is a first sample again.
	if _, ok := dt.Delta("k", 20, base.Add(11*time.Minute)); ok {
		t.Fatal("reaped key must behave as first sample")
	}
}
