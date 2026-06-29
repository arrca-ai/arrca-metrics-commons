// SPDX-License-Identifier: Apache-2.0

// Package langreg holds the runtime/web-language OTLP metric registry + coarse attribute fold.
package langreg

import (
	"sort"

	"github.com/arrca-ai/arrca-metrics-commons/labels"
)

// Sample is one numeric datapoint extracted from OTLP (decoupled from pdata so
// folding is pure and table-testable).
type Sample struct {
	Attrs map[string]string
	Value float64
	TsMs  int64
}

// Folded is one (key,value) after attribute folding. Value is the RAW summed
// value; the receiver applies Scale and rate afterwards.
type Folded struct {
	Key    string
	Value  float64
	TsMs   int64
	Labels map[string]string // identity labels (nil when the signal declares none)
}

// Signal kinds.
const (
	KindSeries = "series"
	KindStatic = "static"
)

// Signal declares how one OTLP runtime metric is transformed and stored.
type Signal struct {
	Key      string                  // stream/static key ("" requires FoldMap)
	Unit     string                  // render-ready unit: MB | count | cores | ratio | conns | /s | flags
	Kind     string                  // KindSeries | KindStatic
	Counter  bool                    // series only: counter→rate
	FoldAttr string                  // datapoint attr to fold by (e.g. jvm.memory.type)
	FoldMap  map[string]string       // fold attr value → key (sums datapoints sharing a value)
	Scale    float64                 // multiply final value; 0 = identity
	Source   string                  // g:lang meta "source": java | go | python | db-client
	Labels   []labels.LabelSpec      // identity-bearing labels (discovery model; values not enumerated)
}

const bytesToMB = 1.0 / (1024 * 1024)

// registry maps OTLP metric name → Signal. A name absent here is dropped: this
// map IS the demux. Java + DB-client flow today; Go/Python are pre-registered so
// they light up automatically once those runtimes are instrumented.
var registry = map[string]Signal{
	// ---- Java / JVM (series) ----
	"jvm.memory.used":               {Unit: "MB", Kind: KindSeries, FoldAttr: "jvm.memory.type", FoldMap: map[string]string{"heap": "jvm_heap_used", "non_heap": "jvm_nonheap_used"}, Scale: bytesToMB, Source: "java"},
	"jvm.memory.used_after_last_gc": {Unit: "MB", Kind: KindSeries, FoldAttr: "jvm.memory.type", FoldMap: map[string]string{"heap": "jvm_heap_after_gc", "non_heap": "jvm_nonheap_after_gc"}, Scale: bytesToMB, Source: "java"},
	"jvm.thread.count":              {Key: "jvm_threads", Unit: "count", Kind: KindSeries, Source: "java"},
	"jvm.class.count":               {Key: "jvm_classes", Unit: "count", Kind: KindSeries, Source: "java"},
	"jvm.cpu.recent_utilization":    {Key: "jvm_cpu_util", Unit: "ratio", Kind: KindSeries, Source: "java"},
	"jvm.cpu.time":                  {Key: "jvm_cpu", Unit: "cores", Kind: KindSeries, Counter: true, Source: "java"},
	"queueSize":                     {Key: "queue_size", Unit: "count", Kind: KindSeries, Source: "java"},
	// ---- Java / JVM (static) ----
	"jvm.memory.limit": {Unit: "MB", Kind: KindStatic, FoldAttr: "jvm.memory.type", FoldMap: map[string]string{"heap": "jvm_heap_limit", "non_heap": "jvm_nonheap_limit"}, Scale: bytesToMB, Source: "java", Labels: []labels.LabelSpec{{Name: "pool", Attr: "jvm.memory.pool.name"}}},
	"jvm.cpu.count":    {Key: "jvm_cpu_count", Unit: "count", Kind: KindStatic, Source: "java"},

	// ---- DB-client pool (series) ----
	"db.client.connections.usage":            {Unit: "conns", Kind: KindSeries, FoldAttr: "state", FoldMap: map[string]string{"used": "db_conn_used", "idle": "db_conn_idle"}, Source: "db-client"},
	"db.client.connections.pending_requests": {Key: "db_conn_pending", Unit: "conns", Kind: KindSeries, Source: "db-client"},
	"db.client.connections.timeouts":         {Key: "db_conn_timeouts", Unit: "/s", Kind: KindSeries, Counter: true, Source: "db-client"},
	// ---- DB-client pool (static) ----
	"db.client.connections.max":      {Key: "db_conn_max", Unit: "conns", Kind: KindStatic, Source: "db-client"},
	"db.client.connections.idle.min": {Key: "db_conn_idle_min", Unit: "conns", Kind: KindStatic, Source: "db-client"},

	// ---- Go runtime (pre-registered; no data today) ----
	"go.goroutine.count": {Key: "go_goroutines", Unit: "count", Kind: KindSeries, Source: "go"},
	"go.memory.used":     {Key: "go_mem_used", Unit: "MB", Kind: KindSeries, Scale: bytesToMB, Source: "go"},
	"go.cpu.time":        {Key: "go_cpu", Unit: "cores", Kind: KindSeries, Counter: true, Source: "go"},
	"go.memory.limit":    {Key: "go_mem_limit", Unit: "MB", Kind: KindStatic, Scale: bytesToMB, Source: "go"},
	"go.processor.limit": {Key: "go_procs", Unit: "count", Kind: KindStatic, Source: "go"},
	"go.config.gogc":     {Key: "go_gogc", Unit: "count", Kind: KindStatic, Source: "go"},

	// ---- Python / CPython runtime (pre-registered; no data today) ----
	"process.runtime.cpython.memory":   {Unit: "MB", Kind: KindSeries, FoldAttr: "type", FoldMap: map[string]string{"rss": "py_mem_rss", "vms": "py_mem_vms"}, Scale: bytesToMB, Source: "python"},
	"process.runtime.cpython.cpu.time": {Key: "py_cpu", Unit: "cores", Kind: KindSeries, Counter: true, Source: "python"},
	"cpython.gc.collections":           {Key: "py_gc_collections", Unit: "/s", Kind: KindSeries, Counter: true, Source: "python"},
}

// Lookup returns the Signal for an OTLP metric name; ok=false → drop.
func Lookup(name string) (Signal, bool) {
	s, ok := registry[name]
	return s, ok
}

// EffScale is Scale, or 1.0 when Scale is zero.
func (s Signal) EffScale() float64 {
	if s.Scale == 0 {
		return 1.0
	}
	return s.Scale
}

// Fold groups+sums datapoints by folded key. For a fold signal, each datapoint's
// FoldAttr is mapped through FoldMap (datapoints with a missing/unmapped attr are
// dropped) and values sharing a key are summed; the latest TsMs wins. For a
// non-fold signal, each datapoint passes through under s.Key. Returned slice is
// ordered by key for determinism. Values are RAW (scale applied by the receiver).
func (s Signal) Fold(dps []Sample) []Folded {
	// Fast path: no fold attr and no identity labels → passthrough per datapoint.
	if s.FoldAttr == "" && len(s.Labels) == 0 {
		if s.Key == "" {
			return nil
		}
		out := make([]Folded, 0, len(dps))
		for _, d := range dps {
			out = append(out, Folded{Key: s.Key, Value: d.Value, TsMs: d.TsMs})
		}
		return out
	}
	type acc struct {
		lbls map[string]string
		sum  float64
		tsMs int64
	}
	m := map[string]*acc{}
	for _, d := range dps {
		base := s.Key
		if s.FoldAttr != "" {
			v, ok := d.Attrs[s.FoldAttr]
			if !ok {
				continue
			}
			base, ok = s.FoldMap[v]
			if !ok {
				continue
			}
		}
		if base == "" {
			continue
		}
		lbls, ok := s.identityLabels(d.Attrs)
		if !ok {
			continue // a declared identity label's attr is missing → drop datapoint
		}
		key := labels.EncodeKey(base, lbls)
		a := m[key]
		if a == nil {
			a = &acc{lbls: lbls}
			m[key] = a
		}
		a.sum += d.Value
		if d.TsMs > a.tsMs {
			a.tsMs = d.TsMs
		}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Folded, 0, len(keys))
	for _, k := range keys {
		out = append(out, Folded{Key: k, Labels: m[k].lbls, Value: m[k].sum, TsMs: m[k].tsMs})
	}
	return out
}

// identityLabels extracts the declared identity labels from a datapoint's attrs.
// ok=false if any declared label's attr is absent (caller drops the datapoint).
// Returns nil when the signal declares no labels.
func (s Signal) identityLabels(attrs map[string]string) (map[string]string, bool) {
	if len(s.Labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(s.Labels))
	for _, l := range s.Labels {
		v, ok := attrs[l.Attr]
		if !ok {
			return nil, false
		}
		out[l.Name] = v
	}
	return out, true
}

// AllSeriesKeys returns every possible stream key for series signals, sorted.
// Note: for signals that declare Labels, the catalog lists the label-free base
// key only; actual emitted keys include the {name=value} suffix, and
// label-dimensioned series are discovered at runtime rather than enumerated here.
func AllSeriesKeys() []string { return keysForKind(KindSeries) }

// AllStaticKeys returns every possible key for static signals, sorted.
// Note: for signals that declare Labels, the catalog lists the label-free base
// key only; actual emitted keys include the {name=value} suffix, and
// label-dimensioned series are discovered at runtime rather than enumerated here.
func AllStaticKeys() []string { return keysForKind(KindStatic) }

func keysForKind(kind string) []string {
	// Note: signals declaring Labels emit base{name=value} keys at runtime;
	// only the label-free base keys are enumerated here.
	seen := map[string]struct{}{}
	for _, s := range registry {
		if s.Kind != kind {
			continue
		}
		if s.FoldAttr == "" {
			seen[s.Key] = struct{}{}
			continue
		}
		for _, k := range s.FoldMap {
			seen[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
