// SPDX-License-Identifier: Apache-2.0
package langx

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

const testID = "container:default/auth-7/app"

func tsNano(ms int64) pcommon.Timestamp { return pcommon.Timestamp(ms * 1_000_000) }

func newTestExistence(t *testing.T, ids ...string) *ingest.ExistenceSet {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	for _, id := range ids {
		mr.SAdd("graph:ids", id)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	e := ingest.NewExistenceSet(rdb, "graph")
	if err := e.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return e
}

// resourceAttrs adds the standard container resource attributes used by testID.
func addContainerResource(ra pcommon.Map) {
	ra.PutStr("k8s.namespace.name", "default")
	ra.PutStr("k8s.pod.name", "auth-7")
	ra.PutStr("k8s.container.name", "app")
}

// buildStaticExport builds a jvm.memory.limit export with a single heap datapoint.
// This mirrors limitExport in receiver_test.go.
func buildStaticExport(heapBytes float64, tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("jvm.memory.limit")
	d := m.SetEmptyGauge().DataPoints().AppendEmpty()
	d.SetDoubleValue(heapBytes)
	d.SetTimestamp(tsNano(tMs))
	d.Attributes().PutStr("jvm.memory.type", "heap")
	d.Attributes().PutStr("jvm.memory.pool.name", "G1 Old Gen")
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

// buildSeriesExport builds a jvm.memory.used export with two heap pools.
// This mirrors jvmMemExport in receiver_test.go.
// Note: jvm.memory.used does NOT have a pool label in langreg, so pool names
// are ignored during Fold and values are summed.
func buildSeriesExport(heap1, heap2 float64, tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("jvm.memory.used")
	dps := m.SetEmptyGauge().DataPoints()
	add := func(pool string, v float64) {
		d := dps.AppendEmpty()
		d.SetDoubleValue(v)
		d.SetTimestamp(tsNano(tMs))
		d.Attributes().PutStr("jvm.memory.type", "heap")
		d.Attributes().PutStr("jvm.memory.pool.name", pool)
	}
	add("Eden", heap1)
	add("Old", heap2)
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

// buildCounterExport builds a jvm.cpu.time Sum (cumulative counter) export.
func buildCounterExport(cpuSec float64, tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("jvm.cpu.time")
	dps := m.SetEmptySum().DataPoints()
	d := dps.AppendEmpty()
	d.SetDoubleValue(cpuSec)
	d.SetTimestamp(tsNano(tMs))
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

// buildUnknownEntityExport builds a jvm.thread.count export for an entity not in
// the ExistenceSet — the result must always be empty.
func buildUnknownEntityExport() []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("k8s.namespace.name", "default")
	ra.PutStr("k8s.pod.name", "ghost-1")
	ra.PutStr("k8s.container.name", "app")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("jvm.thread.count")
	d := m.SetEmptyGauge().DataPoints().AppendEmpty()
	d.SetDoubleValue(5)
	d.SetTimestamp(tsNano(1000))
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

func TestExtract(t *testing.T) {
	const mb = 1024.0 * 1024.0
	exist := newTestExistence(t, testID)
	rates := ingest.NewRateTracker(time.Minute)
	now := time.Unix(10, 0)

	t.Run("static_heap_limit", func(t *testing.T) {
		// jvm.memory.limit: heap=512MB, scale=bytes→MB → ValStr="512"
		// Task 4 added pool label, so key includes {pool=G1 Old Gen}
		data := buildStaticExport(512*mb, 1000)
		_, statics, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(statics) != 1 {
			t.Fatalf("want 1 static, got %d: %+v", len(statics), statics)
		}
		s := statics[0]
		if s.ID != testID {
			t.Errorf("ID = %q, want %q", s.ID, testID)
		}
		if s.Key != "jvm_heap_limit{pool=G1 Old Gen}" {
			t.Errorf("Key = %q, want jvm_heap_limit{pool=G1 Old Gen}", s.Key)
		}
		if s.ValStr != "512" {
			t.Errorf("ValStr = %q, want \"512\"", s.ValStr)
		}
		if s.Unit != "MB" {
			t.Errorf("Unit = %q, want MB", s.Unit)
		}
		if s.TsMs != 1000 {
			t.Errorf("TsMs = %d, want 1000", s.TsMs)
		}
	})

	t.Run("series_heap_used", func(t *testing.T) {
		// jvm.memory.used: Eden=10MB + Old=20MB → heap sum=30MB raw, scale→30.0 MB
		// mirrors: jvmMemExport(10*mb, 20*mb, 4*mb, 1000) → jvm_heap_used=30 in receiver_test.go
		data := buildSeriesExport(10*mb, 20*mb, 1000)
		series, _, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 1 {
			t.Fatalf("want 1 series, got %d: %+v", len(series), series)
		}
		obs := series[0]
		if obs.ID != testID {
			t.Errorf("ID = %q, want %q", obs.ID, testID)
		}
		if obs.Key != "jvm_heap_used" {
			t.Errorf("Key = %q, want jvm_heap_used", obs.Key)
		}
		if obs.Value != 30.0 {
			t.Errorf("Value = %v, want 30.0 (30 MB)", obs.Value)
		}
		if obs.Unit != "MB" {
			t.Errorf("Unit = %q, want MB", obs.Unit)
		}
		if obs.Source != "java" {
			t.Errorf("Source = %q, want java", obs.Source)
		}
		if obs.Counter {
			t.Error("jvm.memory.used must not be Counter")
		}
		if obs.TsMs != 1000 {
			t.Errorf("TsMs = %d, want 1000", obs.TsMs)
		}
	})

	t.Run("counter_cpu_time_rate", func(t *testing.T) {
		// jvm.cpu.time is a cumulative counter; Extract must:
		//   - first call: prime the tracker, return no SeriesObs (ok=false)
		//   - second call: return rate = (200-100) / ((6000-1000)/1000) = 100/5 = 20 cores/s
		ratesLocal := ingest.NewRateTracker(time.Minute)

		// First export: primes the tracker — no series output.
		data1 := buildCounterExport(100.0, 1000)
		series1, _, err := Extract(data1, exist, ratesLocal, now)
		if err != nil {
			t.Fatalf("Extract (first): %v", err)
		}
		if len(series1) != 0 {
			t.Fatalf("first counter export must produce no series, got %d: %+v", len(series1), series1)
		}

		// Second export: dt=5000ms, dv=100 → rate=20 cores/s
		data2 := buildCounterExport(200.0, 6000)
		series2, _, err := Extract(data2, exist, ratesLocal, now)
		if err != nil {
			t.Fatalf("Extract (second): %v", err)
		}
		if len(series2) != 1 {
			t.Fatalf("second counter export must produce 1 series, got %d: %+v", len(series2), series2)
		}
		obs := series2[0]
		if obs.ID != testID {
			t.Errorf("ID = %q, want %q", obs.ID, testID)
		}
		if obs.Key != "jvm_cpu" {
			t.Errorf("Key = %q, want jvm_cpu", obs.Key)
		}
		if obs.Value != 20.0 {
			t.Errorf("Value = %v, want 20.0 (100 units over 5 s)", obs.Value)
		}
		if obs.Unit != "cores" {
			t.Errorf("Unit = %q, want cores", obs.Unit)
		}
		if obs.Source != "java" {
			t.Errorf("Source = %q, want java", obs.Source)
		}
		if !obs.Counter {
			t.Error("jvm.cpu.time must have Counter=true")
		}
		if obs.TsMs != 6000 {
			t.Errorf("TsMs = %d, want 6000", obs.TsMs)
		}
	})

	t.Run("unknown_entity_dropped", func(t *testing.T) {
		// Entity not in ExistenceSet — both slices must be empty.
		data := buildUnknownEntityExport()
		series, statics, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 0 || len(statics) != 0 {
			t.Fatalf("orphan entity must be dropped, got series=%d statics=%d", len(series), len(statics))
		}
	})

	t.Run("unknown_metric_dropped", func(t *testing.T) {
		// A metric name not in langreg — must be silently dropped.
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		addContainerResource(rm.Resource().Attributes())
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("not.a.registered.metric")
		d := m.SetEmptyGauge().DataPoints().AppendEmpty()
		d.SetDoubleValue(1.0)
		d.SetTimestamp(tsNano(1000))
		req := pmetricotlp.NewExportRequestFromMetrics(md)
		data, _ := req.MarshalProto()

		series, statics, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 0 || len(statics) != 0 {
			t.Fatalf("unknown metric must be dropped, got series=%d statics=%d", len(series), len(statics))
		}
	})

	t.Run("malformed_proto_error", func(t *testing.T) {
		_, _, err := Extract([]byte("not-proto"), exist, rates, now)
		if err == nil {
			t.Fatal("malformed proto must return error")
		}
	})
}

func buildMultiPoolLimitExport(tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("jvm.memory.limit")
	dps := m.SetEmptyGauge().DataPoints()
	add := func(typ, pool string, v float64) {
		d := dps.AppendEmpty()
		d.SetDoubleValue(v)
		d.SetTimestamp(tsNano(tMs))
		d.Attributes().PutStr("jvm.memory.type", typ)
		d.Attributes().PutStr("jvm.memory.pool.name", pool)
	}
	add("non_heap", "Metaspace", 1024*1024*1024)
	add("non_heap", "Compressed Class Space", 5*1024*1024)
	add("heap", "G1 Old Gen", 170*1024*1024)
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

func TestExtractPerPoolLimit(t *testing.T) {
	exist := newTestExistence(t, testID)
	_, statics, err := Extract(buildMultiPoolLimitExport(1000), exist, ingest.NewRateTracker(time.Minute), time.Unix(1, 0))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	byKey := map[string]StaticObs{}
	for _, s := range statics {
		byKey[s.Key] = s
	}
	if len(byKey) != 3 {
		t.Fatalf("want 3 per-pool statics, got %d: %+v", len(byKey), statics)
	}
	meta, ok := byKey["jvm_nonheap_limit{pool=Metaspace}"]
	if !ok || meta.Labels["pool"] != "Metaspace" {
		t.Fatalf("Metaspace static missing or unlabelled: %+v", statics)
	}
	if _, ok := byKey["jvm_heap_limit{pool=G1 Old Gen}"]; !ok {
		t.Fatalf("heap pool key missing: %+v", statics)
	}
}
