// SPDX-License-Identifier: Apache-2.0
package cmnx

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

// tsNano converts a unix-ms time to nanoseconds (OTLP timestamps).
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

// buildPodExport mirrors buildExport in receiver_test.go:
// one memory gauge + one cpu counter (two datapoints) + one unknown metric.
// pod:shop/payment-x2k
func buildPodExport(memWS float64, cpu1, cpu2 float64, t1, t2 int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", "payment-x2k")
	sm := rm.ScopeMetrics().AppendEmpty()

	mem := sm.Metrics().AppendEmpty()
	mem.SetName("k8s.pod.memory.working_set")
	g := mem.SetEmptyGauge().DataPoints().AppendEmpty()
	g.SetDoubleValue(memWS)
	g.SetTimestamp(tsNano(t1))

	cpu := sm.Metrics().AppendEmpty()
	cpu.SetName("k8s.pod.cpu.time")
	cd := cpu.SetEmptySum().DataPoints()
	d1 := cd.AppendEmpty()
	d1.SetDoubleValue(cpu1)
	d1.SetTimestamp(tsNano(t1))
	d2 := cd.AppendEmpty()
	d2.SetDoubleValue(cpu2)
	d2.SetTimestamp(tsNano(t2))

	junk := sm.Metrics().AppendEmpty()
	junk.SetName("jvm.memory.used") // not in cmnreg → must be dropped
	junk.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)

	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

// buildNetExport builds a k8s.pod.network.io counter with receive+transmit
// datapoints (pod:shop/payment-x2k).
func buildNetExport(rxBytes, txBytes float64, tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", "payment-x2k")
	sm := rm.ScopeMetrics().AppendEmpty()
	net := sm.Metrics().AppendEmpty()
	net.SetName("k8s.pod.network.io")
	dps := net.SetEmptySum().DataPoints()
	add := func(dir string, v float64) {
		d := dps.AppendEmpty()
		d.SetDoubleValue(v)
		d.SetTimestamp(tsNano(tMs))
		d.Attributes().PutStr("direction", dir)
	}
	add("receive", rxBytes)
	add("transmit", txBytes)
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

func TestExtractNetPerDirection(t *testing.T) {
	exist := newTestExistence(t, "pod:shop/payment-x2k")
	rates := ingest.NewRateTracker(time.Minute)
	if _, _, err := Extract(buildNetExport(0, 0, 1000), exist, rates, time.Unix(1, 0)); err != nil {
		t.Fatalf("prime: %v", err)
	}
	series, _, err := Extract(buildNetExport(1000, 2000, 2000), exist, rates, time.Unix(2, 0))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	byKey := map[string]SeriesObs{}
	for _, s := range series {
		byKey[s.Key] = s
	}
	rx, ok := byKey["net{direction=receive}"]
	if !ok || rx.Labels["direction"] != "receive" {
		t.Fatalf("net receive series missing/unlabeled: %+v", series)
	}
	if _, ok := byKey["net{direction=transmit}"]; !ok {
		t.Fatalf("net transmit series missing: %+v", series)
	}
}

// buildContainerMemLimitExport mirrors buildLimitExport in receiver_test.go.
// container:shop/payment-x2k/app with k8s.container.memory_limit.
func buildContainerMemLimitExport(limitBytes float64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", "payment-x2k")
	ra.PutStr("k8s.container.name", "app")
	sm := rm.ScopeMetrics().AppendEmpty()
	lim := sm.Metrics().AppendEmpty()
	lim.SetName("k8s.container.memory_limit")
	g := lim.SetEmptyGauge().DataPoints().AppendEmpty()
	g.SetDoubleValue(limitBytes)
	g.SetTimestamp(tsNano(1000))
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

// buildContainerCPULimitExport mirrors buildContainerCPUExport in receiver_test.go.
// container:shop/payment-x2k/app with k8s.container.cpu_limit (2 cores).
func buildContainerCPULimitExport(limitCores float64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", "payment-x2k")
	ra.PutStr("k8s.container.name", "app")
	sm := rm.ScopeMetrics().AppendEmpty()
	lim := sm.Metrics().AppendEmpty()
	lim.SetName("k8s.container.cpu_limit")
	g := lim.SetEmptyGauge().DataPoints().AppendEmpty()
	g.SetDoubleValue(limitCores) // already in cores, EffScale=1.0
	g.SetTimestamp(tsNano(1000))
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

const (
	podID       = "pod:shop/payment-x2k"
	containerID = "container:shop/payment-x2k/app"
)

func TestExtract(t *testing.T) {
	now := time.Unix(10, 0)

	t.Run("gauge_mem_series", func(t *testing.T) {
		// k8s.pod.memory.working_set: 1048576 bytes → 1 MB (scale=1/1024/1024)
		// Mirrors: memWS=1048576 → mem v="1" in TestReceiver_WritesSeriesAndMeta
		exist := newTestExistence(t, podID)
		rates := ingest.NewRateTracker(time.Minute)

		data := buildPodExport(1048576, 100, 130, 1000, 4000)
		series, limits, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(limits) != 0 {
			t.Fatalf("pod export must not produce limits, got %d", len(limits))
		}
		// cpu counter: first datapoint primes, second yields rate → 1 series (cpu)
		// mem gauge: 1 series
		// Total: 2 series (mem + cpu rate from second datapoint)
		var memObs *SeriesObs
		var cpuObs *SeriesObs
		for i := range series {
			switch series[i].Key {
			case "mem":
				memObs = &series[i]
			case "cpu":
				cpuObs = &series[i]
			}
		}

		if memObs == nil {
			t.Fatalf("no mem SeriesObs; got %+v", series)
		}
		if memObs.ID != podID {
			t.Errorf("mem ID = %q, want %q", memObs.ID, podID)
		}
		if memObs.Value != 1.0 {
			t.Errorf("mem Value = %v, want 1.0 (MB)", memObs.Value)
		}
		if memObs.Unit != "MB" {
			t.Errorf("mem Unit = %q, want MB", memObs.Unit)
		}
		if memObs.Source != "daemonset" {
			t.Errorf("mem Source = %q, want daemonset", memObs.Source)
		}
		if memObs.Counter {
			t.Error("mem must not be Counter")
		}
		if memObs.TsMs != 1000 {
			t.Errorf("mem TsMs = %d, want 1000", memObs.TsMs)
		}

		// cpu counter: 100@1000ms baseline, 130@4000ms → rate=(130-100)/((4000-1000)/1000)=10 cores
		if cpuObs == nil {
			t.Fatalf("no cpu SeriesObs; got %+v", series)
		}
		if cpuObs.ID != podID {
			t.Errorf("cpu ID = %q, want %q", cpuObs.ID, podID)
		}
		if cpuObs.Value != 10.0 {
			t.Errorf("cpu Value = %v, want 10.0 (cores)", cpuObs.Value)
		}
		if cpuObs.Unit != "cores" {
			t.Errorf("cpu Unit = %q, want cores", cpuObs.Unit)
		}
		if cpuObs.Source != "daemonset" {
			t.Errorf("cpu Source = %q, want daemonset", cpuObs.Source)
		}
		if !cpuObs.Counter {
			t.Error("cpu must have Counter=true")
		}
		if cpuObs.TsMs != 4000 {
			t.Errorf("cpu TsMs = %d, want 4000", cpuObs.TsMs)
		}
	})

	t.Run("counter_rate_two_exports", func(t *testing.T) {
		// Counter rate: first export primes, second yields rate.
		// cpu: 100@1000ms, 130@4000ms → rate=10 cores
		// Mirrors two-export pattern in receiver_test.go (same math, different structure).
		exist := newTestExistence(t, podID)
		rates := ingest.NewRateTracker(time.Minute)

		// Send only cpu counter, two separate exports (each with one datapoint).
		buildOneCPU := func(val float64, tMs int64) []byte {
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			ra := rm.Resource().Attributes()
			ra.PutStr("k8s.namespace.name", "shop")
			ra.PutStr("k8s.pod.name", "payment-x2k")
			sm := rm.ScopeMetrics().AppendEmpty()
			cpu := sm.Metrics().AppendEmpty()
			cpu.SetName("k8s.pod.cpu.time")
			d := cpu.SetEmptySum().DataPoints().AppendEmpty()
			d.SetDoubleValue(val)
			d.SetTimestamp(tsNano(tMs))
			req := pmetricotlp.NewExportRequestFromMetrics(md)
			b, _ := req.MarshalProto()
			return b
		}

		// First export: primes the tracker → no series output.
		s1, l1, err := Extract(buildOneCPU(100, 1000), exist, rates, now)
		if err != nil {
			t.Fatalf("first Extract: %v", err)
		}
		if len(s1) != 0 || len(l1) != 0 {
			t.Fatalf("first counter export must produce nothing, got series=%d limits=%d", len(s1), len(l1))
		}

		// Second export: dt=3000ms, dv=30 → rate=10 cores.
		s2, _, err := Extract(buildOneCPU(130, 4000), exist, rates, now)
		if err != nil {
			t.Fatalf("second Extract: %v", err)
		}
		if len(s2) != 1 {
			t.Fatalf("second counter export must produce 1 series, got %d: %+v", len(s2), s2)
		}
		obs := s2[0]
		if obs.Value != 10.0 {
			t.Errorf("cpu rate = %v, want 10.0 cores", obs.Value)
		}
		if obs.TsMs != 4000 {
			t.Errorf("cpu TsMs = %d, want 4000", obs.TsMs)
		}
	})

	t.Run("mem_limit_obs", func(t *testing.T) {
		// k8s.container.memory_limit: 2*1024*1024 bytes → 2 MB
		// Mirrors TestReceiver_WritesContainerLimit: meta["limit"] == "2"
		// Extract returns LimitObs{ID: containerID, Key: "mem", Value: 2.0}
		exist := newTestExistence(t, containerID)
		rates := ingest.NewRateTracker(time.Minute)

		data := buildContainerMemLimitExport(2 * 1024 * 1024)
		series, limits, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 0 {
			t.Fatalf("limit export must not produce series, got %d: %+v", len(series), series)
		}
		if len(limits) != 1 {
			t.Fatalf("want 1 LimitObs, got %d: %+v", len(limits), limits)
		}
		l := limits[0]
		if l.ID != containerID {
			t.Errorf("limit ID = %q, want %q", l.ID, containerID)
		}
		if l.Key != "mem" {
			t.Errorf("limit Key = %q, want \"mem\"", l.Key)
		}
		if l.Value != 2.0 {
			t.Errorf("limit Value = %v, want 2.0 (MB)", l.Value)
		}
	})

	t.Run("cpu_limit_obs", func(t *testing.T) {
		// k8s.container.cpu_limit: 2 cores, EffScale=1.0
		// LimitObs{ID: containerID, Key: "cpu", Value: 2.0}
		exist := newTestExistence(t, containerID)
		rates := ingest.NewRateTracker(time.Minute)

		data := buildContainerCPULimitExport(2.0)
		series, limits, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 0 {
			t.Fatalf("limit export must not produce series, got %d", len(series))
		}
		if len(limits) != 1 {
			t.Fatalf("want 1 LimitObs, got %d: %+v", len(limits), limits)
		}
		l := limits[0]
		if l.ID != containerID {
			t.Errorf("limit ID = %q, want %q", l.ID, containerID)
		}
		if l.Key != "cpu" {
			t.Errorf("limit Key = %q, want \"cpu\"", l.Key)
		}
		if l.Value != 2.0 {
			t.Errorf("limit Value = %v, want 2.0 (cores)", l.Value)
		}
	})

	t.Run("orphan_drop", func(t *testing.T) {
		// Entity not in ExistenceSet → both slices must be empty.
		// Mirrors TestReceiver_DropsUnknownEntity.
		exist := newTestExistence(t) // empty
		rates := ingest.NewRateTracker(time.Minute)

		data := buildPodExport(1048576, 100, 130, 1000, 4000)
		series, limits, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 0 || len(limits) != 0 {
			t.Fatalf("orphan entity must be dropped, got series=%d limits=%d", len(series), len(limits))
		}
	})

	t.Run("unknown_metric_drop", func(t *testing.T) {
		// Metric not in cmnreg → silently dropped.
		exist := newTestExistence(t, podID)
		rates := ingest.NewRateTracker(time.Minute)

		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		ra := rm.Resource().Attributes()
		ra.PutStr("k8s.namespace.name", "shop")
		ra.PutStr("k8s.pod.name", "payment-x2k")
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("not.a.registered.metric")
		d := m.SetEmptyGauge().DataPoints().AppendEmpty()
		d.SetDoubleValue(1.0)
		d.SetTimestamp(tsNano(1000))
		req := pmetricotlp.NewExportRequestFromMetrics(md)
		data, _ := req.MarshalProto()

		series, limits, err := Extract(data, exist, rates, now)
		if err != nil {
			t.Fatalf("Extract: %v", err)
		}
		if len(series) != 0 || len(limits) != 0 {
			t.Fatalf("unknown metric must be dropped, got series=%d limits=%d", len(series), len(limits))
		}
	})

	t.Run("malformed_proto_error", func(t *testing.T) {
		exist := newTestExistence(t)
		rates := ingest.NewRateTracker(time.Minute)
		_, _, err := Extract([]byte("not-a-protobuf"), exist, rates, now)
		if err == nil {
			t.Fatal("malformed proto must return error")
		}
	})
}
