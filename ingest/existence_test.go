// internal/metrics/existence_test.go
package ingest

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()}), mr
}

func TestExistence_RefreshAndHas(t *testing.T) {
	rdb, mr := newTestRedis(t)
	ctx := context.Background()
	mr.SAdd("graph:ids", "pod:shop/p", "node:node-a")

	e := NewExistenceSet(rdb, "graph")
	if e.Has("pod:shop/p") {
		t.Fatal("must be empty before first Refresh")
	}
	if err := e.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !e.Has("pod:shop/p") || !e.Has("node:node-a") {
		t.Fatal("ids should be present after Refresh")
	}
	if e.Has("pod:shop/unknown") {
		t.Fatal("unknown id must be absent")
	}

	// appears later → present after next Refresh
	mr.SAdd("graph:ids", "pod:shop/late")
	if err := e.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !e.Has("pod:shop/late") {
		t.Fatal("late id should appear after refresh")
	}
}
