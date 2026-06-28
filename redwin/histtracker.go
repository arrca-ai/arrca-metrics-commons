// SPDX-License-Identifier: Apache-2.0
package redwin

import (
	"hash/fnv"
	"sync"
	"time"
)

type histSample struct {
	count   uint64
	buckets []uint64
	tNano   int64
	seenAt  time.Time
}

type histShard struct {
	mu sync.Mutex
	m  map[string]*histSample
}

// HistTracker holds the previous cumulative histogram per series, lock-striped
// across shards so unrelated series never contend on a global mutex.
type HistTracker struct {
	shards []histShard
}

// NewHistTracker builds a tracker with the given shard count (clamped to >=1).
func NewHistTracker(shards int) *HistTracker {
	if shards < 1 {
		shards = 1
	}
	t := &HistTracker{shards: make([]histShard, shards)}
	for i := range t.shards {
		t.shards[i].m = make(map[string]*histSample)
	}
	return t
}

func (t *HistTracker) shardIndex(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(t.shards)))
}

// Delta records the new cumulative sample and returns the delta vs the previous
// one. ok=false on first sample, counter reset, bucket-shape change, or dt<=0.
func (t *HistTracker) Delta(key string, count uint64, buckets []uint64, tNano int64, now time.Time) (dCount uint64, dBuckets []uint64, dtSec float64, ok bool) {
	sh := &t.shards[t.shardIndex(key)]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	prev := sh.m[key]
	cur := &histSample{count: count, buckets: append([]uint64(nil), buckets...), tNano: tNano, seenAt: now}
	sh.m[key] = cur

	if prev == nil || count < prev.count || len(buckets) != len(prev.buckets) {
		return 0, nil, 0, false
	}
	dtSec = float64(tNano-prev.tNano) / 1e9
	if dtSec <= 0 {
		return 0, nil, 0, false
	}
	db := make([]uint64, len(buckets))
	for i := range buckets {
		if buckets[i] < prev.buckets[i] {
			return 0, nil, 0, false // per-bucket reset
		}
		db[i] = buckets[i] - prev.buckets[i]
	}
	return count - prev.count, db, dtSec, true
}

// Reap drops series untouched for longer than ttl and returns how many.
func (t *HistTracker) Reap(now time.Time, ttl time.Duration) int {
	n := 0
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for k, s := range sh.m {
			if now.Sub(s.seenAt) > ttl {
				delete(sh.m, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}
