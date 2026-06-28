package ingest

import (
	"reflect"
	"testing"
)

func TestOwnedSubjects_SingleShardWildcard(t *testing.T) {
	got, err := OwnedSubjects("metrics.part", 256, 0, 1)
	if err != nil || !reflect.DeepEqual(got, []string{"metrics.part.*"}) {
		t.Fatalf("got (%v,%v), want ([metrics.part.*],nil)", got, err)
	}
}

func TestOwnedSubjects_RangeSplit(t *testing.T) {
	// 4 partitions over 2 shards → shard 1 owns [2,4)
	got, err := OwnedSubjects("metrics.part", 4, 1, 2)
	want := []string{"metrics.part.2", "metrics.part.3"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got (%v,%v), want (%v,nil)", got, err, want)
	}
}

func TestOwnedSubjects_BadShardIndex(t *testing.T) {
	if _, err := OwnedSubjects("metrics.part", 256, 5, 4); err == nil {
		t.Fatal("shardIndex >= shardCount must error")
	}
}

func TestOwnedSubjects_SingleShardNonZeroIndex(t *testing.T) {
	if _, err := OwnedSubjects("p", 256, 1, 1); err == nil {
		t.Fatal("shardIndex != 0 with shardCount=1 must error")
	}
}

func TestOwnedSubjects_InvalidPartitions(t *testing.T) {
	if _, err := OwnedSubjects("p", 0, 0, 1); err == nil {
		t.Fatal("partitions <= 0 must error")
	}
}

func TestShardIndexFromPodName(t *testing.T) {
	i, err := ShardIndexFromPodName("graph-metrics-3")
	if err != nil || i != 3 {
		t.Fatalf("got (%d,%v), want (3,nil)", i, err)
	}
	if _, err := ShardIndexFromPodName("noordinal"); err == nil {
		t.Fatal("name without ordinal must error")
	}
}

func TestShardIndexFromPodName_TrailingDash(t *testing.T) {
	if _, err := ShardIndexFromPodName("graph-metrics-"); err == nil {
		t.Fatal("trailing dash must error")
	}
}

func TestShardIndexFromPodName_NonNumericSuffix(t *testing.T) {
	if _, err := ShardIndexFromPodName("graph-metrics-abc"); err == nil {
		t.Fatal("non-numeric suffix must error")
	}
}
