// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"hash/fnv"
	"sync"
	"time"
)

type deltaState struct {
	last   float64
	seenAt time.Time
}

type deltaShard struct {
	mu sync.Mutex
	m  map[string]deltaState
}

// DeltaTracker converts cumulative counters to per-sample deltas, keeping the
// last value per series key in memory, lock-striped across shards so unrelated
// series never contend on a global mutex. Safe for concurrent use.
type DeltaTracker struct {
	shards []deltaShard
}

// NewDeltaTracker builds a tracker with the given shard count (clamped to >=1).
func NewDeltaTracker(shards int) *DeltaTracker {
	if shards < 1 {
		shards = 1
	}
	t := &DeltaTracker{shards: make([]deltaShard, shards)}
	for i := range t.shards {
		t.shards[i].m = make(map[string]deltaState)
	}
	return t
}

func (t *DeltaTracker) shardIndex(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(t.shards)))
}

// Delta records the new cumulative sample and returns the increase since the
// previous sample for key. ok=false on first sample or counter reset
// (value<last); the caller emits nothing in those cases. The new sample always
// becomes the baseline.
func (t *DeltaTracker) Delta(key string, value float64, now time.Time) (float64, bool) {
	sh := &t.shards[t.shardIndex(key)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	prev, seen := sh.m[key]
	sh.m[key] = deltaState{last: value, seenAt: now}
	if !seen || value < prev.last {
		return 0, false
	}
	return value - prev.last, true
}

// Reap drops state not updated within ttl and returns the count removed.
func (t *DeltaTracker) Reap(now time.Time, ttl time.Duration) int {
	n := 0
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for k, st := range sh.m {
			if now.Sub(st.seenAt) > ttl {
				delete(sh.m, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}
