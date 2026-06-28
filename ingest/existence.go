// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ExistenceSet holds an in-memory snapshot of the topology's entity-id set
// (<prefix>:ids), refreshed periodically. Datapoints whose id is absent are
// dropped to avoid orphan streams.
type ExistenceSet struct {
	rdb    *redis.Client
	idsKey string
	mu     sync.RWMutex
	set    map[string]struct{}
}

func NewExistenceSet(rdb *redis.Client, keyPrefix string) *ExistenceSet {
	return &ExistenceSet{rdb: rdb, idsKey: keyPrefix + ":ids", set: make(map[string]struct{})}
}

// Refresh loads the full id set and swaps it in atomically.
func (e *ExistenceSet) Refresh(ctx context.Context) error {
	ids, err := e.rdb.SMembers(ctx, e.idsKey).Result()
	if err != nil {
		return err
	}
	next := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		next[id] = struct{}{}
	}
	e.mu.Lock()
	e.set = next
	e.mu.Unlock()
	return nil
}

// Has reports whether id is in the most recent snapshot.
func (e *ExistenceSet) Has(id string) bool {
	e.mu.RLock()
	_, ok := e.set[id]
	e.mu.RUnlock()
	return ok
}

// Run refreshes immediately, then every interval until ctx is canceled.
func (e *ExistenceSet) Run(ctx context.Context, logger *slog.Logger, interval time.Duration) {
	if err := e.Refresh(ctx); err != nil {
		logger.Warn("existence refresh failed", slog.Any("err", err))
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.Refresh(ctx); err != nil {
				logger.Warn("existence refresh failed", slog.Any("err", err))
			}
		}
	}
}
