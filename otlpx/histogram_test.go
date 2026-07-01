// SPDX-License-Identifier: Apache-2.0

package otlpx

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

func histExport(ns, pod, c, name string, bounds []float64, counts []uint64, tNano int64) []byte {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", ns)
	rm.Resource().Attributes().PutStr("k8s.pod.name", pod)
	rm.Resource().Attributes().PutStr("k8s.container.name", c)
	met := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	met.SetName(name)
	h := met.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := h.DataPoints().AppendEmpty()
	dp.ExplicitBounds().FromRaw(bounds)
	dp.BucketCounts().FromRaw(counts)
	dp.SetTimestamp(pcommon.Timestamp(tNano))
	dp.Attributes().PutStr("http.route", "/api")
	b, _ := pmetricotlp.NewExportRequestFromMetrics(m).MarshalProto()
	return b
}

func TestExtract_Histogram(t *testing.T) {
	reg := NewRegistry([]MetricConfig{{Name: "http.server.duration", Level: ingest.LevelContainer}})
	bounds := []float64{10, 50, 100, 500}
	counts := []uint64{60, 25, 10, 3, 2}
	obs, err := Extract(histExport("ns", "p", "app", "http.server.duration", bounds, counts, 1_000_000), reg)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("want 1 obs, got %d", len(obs))
	}
	o := obs[0]
	if o.Type != "histogram" || o.Name != "http.server.duration" || o.ResolveID != "container:ns/p/app" {
		t.Fatalf("identity wrong: %+v", o)
	}
	if len(o.Bounds) != 4 || o.Bounds[0] != 10 || o.Bounds[3] != 500 {
		t.Fatalf("bounds wrong: %+v", o.Bounds)
	}
	if len(o.Counts) != 5 || o.Counts[0] != 60 || o.Counts[4] != 2 {
		t.Fatalf("counts wrong: %+v", o.Counts)
	}
	if o.SeriesKey == "" || o.Labels["http.route"] != "/api" {
		t.Fatalf("serieskey/labels wrong: %+v", o)
	}
}
