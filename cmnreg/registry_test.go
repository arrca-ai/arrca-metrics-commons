// SPDX-License-Identifier: Apache-2.0

package cmnreg

import (
	"testing"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

func TestNetIsLabeledByDirection(t *testing.T) {
	s, ok := Lookup("k8s.pod.network.io")
	if !ok || s.Key != "net" || len(s.Labels) != 1 || s.Labels[0].Name != "direction" || s.Labels[0].Attr != "direction" {
		t.Fatalf("net must be base 'net' + {direction} label: %+v", s)
	}
	get := func(m map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	lbls, ok := s.IdentityLabels(get(map[string]string{"direction": "receive"}))
	if !ok || lbls["direction"] != "receive" {
		t.Fatalf("IdentityLabels wrong: %v %v", lbls, ok)
	}
	if _, ok := s.IdentityLabels(get(map[string]string{})); ok {
		t.Fatalf("missing direction attr must drop (ok=false)")
	}
	cpu, _ := Lookup("container.cpu.time")
	if l, ok := cpu.IdentityLabels(get(map[string]string{})); !ok || l != nil {
		t.Fatalf("unlabeled metric must yield (nil,true): %v %v", l, ok)
	}
}

func TestLookup_ClaimedAndDropped(t *testing.T) {
	if _, ok := Lookup("container.cpu.time"); !ok {
		t.Fatal("container.cpu.time should be claimed")
	}
	if _, ok := Lookup("http.server.request.duration"); ok {
		t.Fatal("histogram metric must be dropped (not in C1 table)")
	}
}

func TestResolveKey_NonSplit(t *testing.T) {
	s, _ := Lookup("container.cpu.time")
	k, ok := s.ResolveKey(func(string) (string, bool) { return "", false })
	if !ok || k != "cpu" {
		t.Fatalf("got (%q,%v), want (cpu,true)", k, ok)
	}
}

func TestEffScale(t *testing.T) {
	mem, _ := Lookup("container.memory.working_set")
	if got := mem.EffScale(); got != 1.0/(1024*1024) {
		t.Fatalf("mem scale = %v, want bytes→MB", got)
	}
	cpu, _ := Lookup("container.cpu.time")
	if got := cpu.EffScale(); got != 1.0 {
		t.Fatalf("cpu scale = %v, want 1.0", got)
	}
}

func TestLookup_LimitMetrics(t *testing.T) {
	// Only container limits are emitted by the collector; node allocatable is not.
	for _, name := range []string{"k8s.container.cpu_limit", "k8s.container.memory_limit"} {
		s, ok := Lookup(name)
		if !ok || !s.Limit {
			t.Fatalf("%s: ok=%v Limit=%v, want a claimed limit metric", name, ok, s.Limit)
		}
	}
	// container memory limit scales bytes→MB and keys to mem at container level
	mem, _ := Lookup("k8s.container.memory_limit")
	if mem.Key != "mem" || mem.Level != ingest.LevelContainer || mem.EffScale() != 1.0/(1024*1024) {
		t.Fatalf("memory_limit spec wrong: %+v", mem)
	}
	// container cpu limit keys to cpu at container level, identity scale
	cpu, _ := Lookup("k8s.container.cpu_limit")
	if cpu.Key != "cpu" || cpu.Level != ingest.LevelContainer || cpu.EffScale() != 1.0 {
		t.Fatalf("cpu_limit spec wrong: %+v", cpu)
	}
	// node allocatable is not emitted by the collector → must not be claimed
	if _, ok := Lookup("k8s.node.allocatable_memory"); ok {
		t.Fatal("k8s.node.allocatable_memory is not emitted; must not be claimed")
	}
	// usage metrics are NOT limits
	if s, _ := Lookup("container.cpu.time"); s.Limit {
		t.Fatal("container.cpu.time must not be a limit")
	}
}

func TestRegistry_NodeUsesKubeletstatsSingleValued(t *testing.T) {
	// Node usage must come from single-valued kubeletstats metrics, not the
	// multi-dimensional hostmetrics (system.*), which emit many datapoints per
	// scrape (per cpu/state/device) that collapse onto one series and break
	// rate/value computation.
	cpu, ok := Lookup("k8s.node.cpu.usage")
	if !ok || cpu.Key != "cpu" || cpu.Level != ingest.LevelNode || cpu.Counter {
		t.Fatalf("k8s.node.cpu.usage spec wrong: %+v ok=%v", cpu, ok)
	}
	mem, ok := Lookup("k8s.node.memory.working_set")
	if !ok || mem.Key != "mem" || mem.Level != ingest.LevelNode || mem.EffScale() != bytesToMB {
		t.Fatalf("k8s.node.memory.working_set spec wrong: %+v ok=%v", mem, ok)
	}
	net, ok := Lookup("k8s.node.network.io")
	if !ok || net.Level != ingest.LevelNode || !net.Counter || net.Key != "net" || len(net.Labels) != 1 {
		t.Fatalf("k8s.node.network.io spec wrong: %+v ok=%v", net, ok)
	}
	// multi-dimensional hostmetrics and the non-existent container network are dropped
	for _, dropped := range []string{
		"system.cpu.time", "system.memory.usage", "system.network.io", "container.network.io",
	} {
		if _, ok := Lookup(dropped); ok {
			t.Errorf("%s must no longer be claimed", dropped)
		}
	}
}
