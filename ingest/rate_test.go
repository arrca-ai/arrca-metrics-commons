package ingest

import (
	"testing"
	"time"
)

func TestRate_FirstSampleEmitsNothing(t *testing.T) {
	r := NewRateTracker(time.Minute)
	if _, ok := r.Rate("k", 100, 1000, time.Unix(0, 0)); ok {
		t.Fatal("first sample must not emit")
	}
}

func TestRate_ComputesPerSecond(t *testing.T) {
	r := NewRateTracker(time.Minute)
	r.Rate("k", 100, 1000, time.Unix(0, 0))          // baseline
	v, ok := r.Rate("k", 130, 4000, time.Unix(0, 0)) // +30 over 3s
	if !ok || v != 10 {
		t.Fatalf("got (%v,%v), want (10,true)", v, ok)
	}
}

func TestRate_CounterResetEmitsNothing(t *testing.T) {
	r := NewRateTracker(time.Minute)
	r.Rate("k", 100, 1000, time.Unix(0, 0))
	if _, ok := r.Rate("k", 5, 2000, time.Unix(0, 0)); ok {
		t.Fatal("counter reset (value<prev) must not emit")
	}
	// after reset, the new baseline is in place
	v, ok := r.Rate("k", 25, 3000, time.Unix(0, 0))
	if !ok || v != 20 {
		t.Fatalf("post-reset got (%v,%v), want (20,true)", v, ok)
	}
}

func TestRate_NonPositiveDtSkipped(t *testing.T) {
	r := NewRateTracker(time.Minute)
	r.Rate("k", 100, 2000, time.Unix(0, 0))
	if _, ok := r.Rate("k", 110, 2000, time.Unix(0, 0)); ok {
		t.Fatal("dt==0 must not emit")
	}
}

func TestReap_EvictsIdle(t *testing.T) {
	r := NewRateTracker(10 * time.Second)
	base := time.Unix(100, 0)
	r.Rate("k", 1, 1000, base)
	if n := r.Reap(base.Add(20 * time.Second)); n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}
	// first sample again after eviction → no emit
	if _, ok := r.Rate("k", 5, 5000, base.Add(20*time.Second)); ok {
		t.Fatal("state should have been evicted")
	}
}
