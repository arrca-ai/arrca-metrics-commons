// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func mapOf(kv map[string]string) pcommon.Map {
	m := pcommon.NewMap()
	for k, v := range kv {
		m.PutStr(k, v)
	}
	return m
}

func TestSeriesKey_OrderIndependentAndDistinct(t *testing.T) {
	res := mapOf(map[string]string{"k8s.namespace.name": "shop", "k8s.pod.name": "p"})
	dpA := mapOf(map[string]string{"direction": "receive", "x": "1"})
	dpB := mapOf(map[string]string{"x": "1", "direction": "receive"}) // same attrs, different insert order
	scope := mapOf(map[string]string{})

	k1 := SeriesKey(res, "scope", "v1", scope, "k8s.pod.network.io", dpA)
	k2 := SeriesKey(res, "scope", "v1", scope, "k8s.pod.network.io", dpB)
	if k1 != k2 {
		t.Fatalf("order independence broken: %q != %q", k1, k2)
	}
	if k1 == "" {
		t.Fatal("seriesKey must be non-empty")
	}

	// Different datapoint attr → different key.
	dpC := mapOf(map[string]string{"direction": "transmit", "x": "1"})
	if k3 := SeriesKey(res, "scope", "v1", scope, "k8s.pod.network.io", dpC); k3 == k1 {
		t.Fatal("different dp attrs must yield a different key")
	}
	// Different metric name → different key.
	if k4 := SeriesKey(res, "scope", "v1", scope, "k8s.pod.cpu.time", dpA); k4 == k1 {
		t.Fatal("different metric name must yield a different key")
	}
	// Different resource → different key.
	res2 := mapOf(map[string]string{"k8s.namespace.name": "shop", "k8s.pod.name": "q"})
	if k5 := SeriesKey(res2, "scope", "v1", scope, "k8s.pod.network.io", dpA); k5 == k1 {
		t.Fatal("different resource must yield a different key")
	}
}
