// SPDX-License-Identifier: Apache-2.0
package redwin

import (
	"math"
	"testing"
)

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestPercentileInterpolatesWithinBucket(t *testing.T) {
	// bounds 10,50,100 → 4 buckets; all 100 samples in bucket (10,50].
	counts := []uint64{0, 100, 0, 0}
	bounds := []float64{10, 50, 100}
	if p := Percentile(counts, bounds, 0.50); !approx(p, 30) {
		t.Fatalf("p50=%v want 30", p)
	}
	if p := Percentile(counts, bounds, 0.90); !approx(p, 46) {
		t.Fatalf("p90=%v want 46", p)
	}
	if p := Percentile(counts, bounds, 0.99); !approx(p, 49.6) {
		t.Fatalf("p99=%v want 49.6", p)
	}
}

func TestPercentileEmpty(t *testing.T) {
	if p := Percentile([]uint64{0, 0}, []float64{10}, 0.5); p != 0 {
		t.Fatalf("empty hist must be 0, got %v", p)
	}
}

func TestPercentileInfBucketReturnsLowerBound(t *testing.T) {
	// all mass in the +Inf overflow bucket (index 3, no finite upper bound).
	counts := []uint64{0, 0, 0, 100}
	bounds := []float64{10, 50, 100}
	if p := Percentile(counts, bounds, 0.5); !approx(p, 100) {
		t.Fatalf("p50=%v want 100 (last finite bound)", p)
	}
}

func TestPercentilesTriple(t *testing.T) {
	counts := []uint64{0, 100, 0, 0}
	bounds := []float64{10, 50, 100}
	p50, p90, p99 := Percentiles(counts, bounds)
	if !approx(p50, 30) || !approx(p90, 46) || !approx(p99, 49.6) {
		t.Fatalf("got %v %v %v", p50, p90, p99)
	}
}
