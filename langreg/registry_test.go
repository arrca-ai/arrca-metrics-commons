// SPDX-License-Identifier: Apache-2.0
package langreg

import (
	"sort"
	"testing"
)

func TestLookupHitAndMiss(t *testing.T) {
	if _, ok := Lookup("jvm.thread.count"); !ok {
		t.Fatal("jvm.thread.count must be registered")
	}
	if _, ok := Lookup("not.a.metric"); ok {
		t.Fatal("unknown metric must miss")
	}
}

func TestFoldSumsPoolsByType(t *testing.T) {
	s, _ := Lookup("jvm.memory.used") // FoldAttr jvm.memory.type, Scale bytes→MB
	mb := 1024.0 * 1024.0
	got := s.Fold([]Sample{
		{Attrs: map[string]string{"jvm.memory.type": "heap", "jvm.memory.pool.name": "Eden"}, Value: 10 * mb, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.type": "heap", "jvm.memory.pool.name": "Old"}, Value: 20 * mb, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "Metaspace"}, Value: 4 * mb, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.pool.name": "NoType"}, Value: 999 * mb, TsMs: 5}, // missing fold attr → dropped
	})
	// Fold sums raw values; scale is applied later by the receiver, so Fold returns raw sums.
	want := map[string]float64{"jvm_heap_used": 30 * mb, "jvm_nonheap_used": 4 * mb}
	if len(got) != 2 {
		t.Fatalf("got %d folded keys, want 2: %+v", len(got), got)
	}
	for _, f := range got {
		if want[f.Key] != f.Value {
			t.Fatalf("key %s = %v, want %v", f.Key, f.Value, want[f.Key])
		}
	}
}

func TestFoldNonFoldPassThrough(t *testing.T) {
	s, _ := Lookup("jvm.thread.count") // no FoldAttr
	got := s.Fold([]Sample{{Attrs: map[string]string{}, Value: 42, TsMs: 7}})
	if len(got) != 1 || got[0].Key != "jvm_threads" || got[0].Value != 42 {
		t.Fatalf("non-fold passthrough wrong: %+v", got)
	}
}

func TestSeriesVsStaticKind(t *testing.T) {
	if s, _ := Lookup("jvm.thread.count"); s.Kind != KindSeries {
		t.Fatal("thread.count must be series")
	}
	if s, _ := Lookup("jvm.memory.limit"); s.Kind != KindStatic {
		t.Fatal("memory.limit must be static")
	}
}

func TestKeyCatalogs(t *testing.T) {
	sk := AllSeriesKeys()
	if !sort.StringsAreSorted(sk) {
		t.Fatal("AllSeriesKeys must be sorted")
	}
	has := func(xs []string, want string) bool {
		for _, x := range xs {
			if x == want {
				return true
			}
		}
		return false
	}
	if !has(sk, "jvm_heap_used") || !has(sk, "db_conn_used") {
		t.Fatalf("series catalog missing folded keys: %v", sk)
	}
	if !has(AllStaticKeys(), "jvm_heap_limit") {
		t.Fatalf("static catalog missing jvm_heap_limit: %v", AllStaticKeys())
	}
	// sanity: catalogs are disjoint
	for _, a := range sk {
		if has(AllStaticKeys(), a) {
			t.Fatalf("key %s in both series and static catalogs", a)
		}
	}
}
