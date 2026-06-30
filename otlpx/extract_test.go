// SPDX-License-Identifier: Apache-2.0

package otlpx

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

// testReg is a small registry exercising sum, gauge, limit, and a node level.
var testReg = NewRegistry([]MetricConfig{
	{Name: "k8s.pod.cpu.time", Level: ingest.LevelPod},
	{Name: "k8s.container.cpu_limit", Level: ingest.LevelContainer, IsLimit: true},
})

func podCPUExport(ns, pod string, v float64, tNano int64) []byte {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", ns)
	rm.Resource().Attributes().PutStr("k8s.pod.name", pod)
	met := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	met.SetName("k8s.pod.cpu.time")
	met.SetUnit("s")
	s := met.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := s.DataPoints().AppendEmpty()
	dp.SetDoubleValue(v)
	dp.SetTimestamp(pcommon.Timestamp(tNano))
	b, _ := pmetricotlp.NewExportRequestFromMetrics(m).MarshalProto()
	return b
}

func TestExtract_SumObs(t *testing.T) {
	obs, err := Extract(podCPUExport("ns", "p", 42, 7_000_000), testReg)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("want 1 obs, got %d", len(obs))
	}
	o := obs[0]
	if o.ResolveID != "pod:ns/p" || o.Name != "k8s.pod.cpu.time" || o.Type != "sum" || o.IsLimit {
		t.Fatalf("bad obs: %+v", o)
	}
	if o.Raw != 42 || o.TsMs != 7 || o.Unit != "s" || o.SeriesKey == "" {
		t.Fatalf("bad values: %+v", o)
	}
}

func TestExtract_DropsUnknownMetric(t *testing.T) {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", "ns")
	rm.Resource().Attributes().PutStr("k8s.pod.name", "p")
	met := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	met.SetName("not.in.config")
	met.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)
	b, _ := pmetricotlp.NewExportRequestFromMetrics(m).MarshalProto()

	obs, err := Extract(b, testReg)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(obs) != 0 {
		t.Fatalf("unknown metric must be dropped, got %d", len(obs))
	}
}

func TestExtract_LimitFlaggedFromConfig(t *testing.T) {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", "ns")
	rm.Resource().Attributes().PutStr("k8s.pod.name", "p")
	rm.Resource().Attributes().PutStr("k8s.container.name", "app")
	met := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	met.SetName("k8s.container.cpu_limit")
	met.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(2)
	b, _ := pmetricotlp.NewExportRequestFromMetrics(m).MarshalProto()

	obs, err := Extract(b, testReg)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(obs) != 1 || !obs[0].IsLimit || obs[0].ResolveID != "container:ns/p/app" {
		t.Fatalf("limit obs wrong: %+v", obs)
	}
}
