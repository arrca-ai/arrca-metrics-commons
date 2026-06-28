// SPDX-License-Identifier: Apache-2.0
package events

import "testing"

func TestPartitionDeterministicAndInRange(t *testing.T) {
	id := "container:default/api-1/app"
	a := Partition(id, 256)
	b := Partition(id, 256)
	if a != b {
		t.Fatalf("not deterministic: %d != %d", a, b)
	}
	if a < 0 || a >= 256 {
		t.Fatalf("out of range: %d", a)
	}
}

func TestPartitionSingle(t *testing.T) {
	if got := Partition("anything", 1); got != 0 {
		t.Fatalf("n=1 must map to 0, got %d", got)
	}
	if got := Partition("anything", 0); got != 0 {
		t.Fatalf("n=0 must map to 0, got %d", got)
	}
}

func TestPartitionSpreads(t *testing.T) {
	seen := map[int]bool{}
	for _, s := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		seen[Partition("container:default/"+s+"/app", 8)] = true
	}
	if len(seen) < 3 {
		t.Fatalf("expected spread across buckets, got %d distinct", len(seen))
	}
}
