// SPDX-License-Identifier: Apache-2.0

// Package cmnreg implements the C1 resource-usage consumer: it subscribes to
// otel-metrics-hub's NATS partition subjects, demuxes OTLP metrics by name, and
// writes render-ready Redis Streams to the shared graph-redis.
package cmnreg

import (
	"github.com/arrca-ai/arrca-metrics-commons/ingest"
	"github.com/arrca-ai/arrca-metrics-commons/labels"
)

// MetricSpec declares how one OTLP metric is transformed and stored.
type MetricSpec struct {
	Key     string             // stream base key: cpu|mem|net
	Unit    string             // render-ready unit: cores|MB|B/s
	Level   ingest.Level       // which attrs resolve the id
	Counter bool               // true → counter→rate; false → gauge as-is
	Labels  []labels.LabelSpec // identity-bearing labels (discovery model; e.g. net direction)
	Scale   float64            // multiply the final value; 0 means identity (1.0)
	Source  string             // g:meta "source" field
	Limit   bool               // true → write to g:meta.limit (not a series)
}

const bytesToMB = 1.0 / (1024 * 1024)

// registry maps OTLP metric name → spec. A name absent here is dropped: this map
// IS the demux. C4 (RED) / C5 (runtime) extend it later with new rows.
var registry = map[string]MetricSpec{
	"container.cpu.time":           {Key: "cpu", Unit: "cores", Level: ingest.LevelContainer, Counter: true, Source: "daemonset"},
	"container.memory.working_set": {Key: "mem", Unit: "MB", Level: ingest.LevelContainer, Scale: bytesToMB, Source: "daemonset"},
	// No container.network.io: containers share the pod network namespace, so the
	// collector emits network only at pod/node level.
	"k8s.pod.cpu.time":           {Key: "cpu", Unit: "cores", Level: ingest.LevelPod, Counter: true, Source: "daemonset"},
	"k8s.pod.memory.working_set": {Key: "mem", Unit: "MB", Level: ingest.LevelPod, Scale: bytesToMB, Source: "daemonset"},
	"k8s.pod.network.io":         {Key: "net", Unit: "B/s", Level: ingest.LevelPod, Counter: true, Labels: []labels.LabelSpec{{Name: "direction", Attr: "direction"}}, Source: "daemonset"},
	// Node usage comes from single-valued kubeletstats k8s.node.* metrics, NOT the
	// multi-dimensional hostmetrics system.* (per cpu/state/device), which collapse
	// many datapoints onto one series and break rate/value computation.
	"k8s.node.cpu.usage":          {Key: "cpu", Unit: "cores", Level: ingest.LevelNode, Source: "kubeletstats"}, // gauge (cores), no rate
	"k8s.node.memory.working_set": {Key: "mem", Unit: "MB", Level: ingest.LevelNode, Scale: bytesToMB, Source: "kubeletstats"},
	"k8s.node.network.io":         {Key: "net", Unit: "B/s", Level: ingest.LevelNode, Counter: true, Labels: []labels.LabelSpec{{Name: "direction", Attr: "direction"}}, Source: "kubeletstats"},
	// Limits (g:meta.limit). Only container limits are emitted; node allocatable is not.
	"k8s.container.cpu_limit":    {Key: "cpu", Unit: "cores", Level: ingest.LevelContainer, Limit: true, Source: "cluster"},
	"k8s.container.memory_limit": {Key: "mem", Unit: "MB", Level: ingest.LevelContainer, Scale: bytesToMB, Limit: true, Source: "cluster"},
}

// Lookup returns the spec for an OTLP metric name; ok=false → drop the metric.
func Lookup(name string) (MetricSpec, bool) {
	s, ok := registry[name]
	return s, ok
}

// ResolveKey returns the base stream key for a datapoint. ok=false → drop.
func (s MetricSpec) ResolveKey(getAttr func(string) (string, bool)) (string, bool) {
	return s.Key, s.Key != ""
}

// IdentityLabels extracts the declared identity labels from a datapoint. ok=false
// if any declared label's attr is absent (drop the datapoint). nil when none.
func (s MetricSpec) IdentityLabels(getAttr func(string) (string, bool)) (map[string]string, bool) {
	if len(s.Labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(s.Labels))
	for _, l := range s.Labels {
		v, ok := getAttr(l.Attr)
		if !ok {
			return nil, false
		}
		out[l.Name] = v
	}
	return out, true
}

// EffScale is Scale, or 1.0 when Scale is zero.
func (s MetricSpec) EffScale() float64 {
	if s.Scale == 0 {
		return 1.0
	}
	return s.Scale
}
