// SPDX-License-Identifier: Apache-2.0

package cmnx

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

// buildPodCPU builds a one-datapoint OTLP export for k8s.pod.cpu.time (a Sum
// counter) with cumulative value v at time tNano for pod ns/name.
func buildPodCPU(ns, pod string, v float64, tNano int64) []byte {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", ns)
	rm.Resource().Attributes().PutStr("k8s.pod.name", pod)
	met := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	met.SetName("k8s.pod.cpu.time")
	dp := met.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetDoubleValue(v)
	dp.SetTimestamp(pcommon.Timestamp(tNano))
	b, _ := pmetricotlp.NewExportRequestFromMetrics(m).MarshalProto()
	return b
}

func TestExtractRaw_CounterEmitsUnscaledRaw(t *testing.T) {
	raw, _, err := ExtractRaw(buildPodCPU("ns", "p", 42.5, 7_000_000), newTestExistence(t, "pod:ns/p"))
	if err != nil {
		t.Fatalf("ExtractRaw: %v", err)
	}
	if len(raw) != 1 {
		t.Fatalf("want 1 obs, got %d", len(raw))
	}
	o := raw[0]
	if o.ID != "pod:ns/p" || o.Key != "cpu" || !o.Counter {
		t.Fatalf("bad identity: %+v", o)
	}
	if o.Raw != 42.5 {
		t.Fatalf("want Raw=42.5 (unscaled), got %v", o.Raw)
	}
	if o.Scale != 1.0 {
		t.Fatalf("want Scale=1.0 for cpu, got %v", o.Scale)
	}
	if o.TsMs != 7 { // 7_000_000 ns / 1e6
		t.Fatalf("want TsMs=7, got %d", o.TsMs)
	}
}
