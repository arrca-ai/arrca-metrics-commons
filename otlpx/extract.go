// SPDX-License-Identifier: Apache-2.0

package otlpx

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

// Obs is one extracted datapoint. Raw is the unscaled value as received; callers
// apply rate/scale. SeriesKey is an opaque per-series identity (resource + scope +
// metric name + dp attrs) suitable as a detector state key. Labels are the dp
// attributes.
type Obs struct {
	ResolveID, SeriesKey, Name, Unit, Type string
	IsLimit                                bool
	Raw                                    float64
	TsMs                                   int64
	Labels                                 map[string]string
	Bounds                                 []float64 // histogram only: ExplicitBounds (len N)
	Counts                                 []uint64  // histogram only: cumulative BucketCounts (len N+1)
}

// Extract decodes one OTLP export into observations, dropping metrics not in reg.
// Stateless: no rate computation, no existence gating.
func Extract(data []byte, reg Registry) ([]Obs, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(data); err != nil {
		return nil, err
	}
	var out []Obs
	rms := req.Metrics().ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resAttrs := rm.Resource().Attributes()
		getRes := ingest.AttrGetter(resAttrs)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				cfg, ok := reg.Lookup(m.Name())
				if !ok {
					continue
				}
				id, ok := ingest.ResolveID(cfg.Level, getRes)
				if !ok {
					continue
				}
				switch m.Type() {
				case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
					dps, typ, _ := numberDPs(m)
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						raw, ok := ingest.NumberValue(dp)
						if !ok {
							continue
						}
						out = append(out, Obs{
							ResolveID: id,
							SeriesKey: ingest.SeriesKey(resAttrs, scope.Name(), scope.Version(), scope.Attributes(), m.Name(), dp.Attributes()),
							Name:      m.Name(),
							Unit:      m.Unit(),
							Type:      typ,
							IsLimit:   cfg.IsLimit,
							Raw:       raw,
							TsMs:      int64(dp.Timestamp()) / 1_000_000,
							Labels:    attrsToMap(dp.Attributes()),
						})
					}
				case pmetric.MetricTypeHistogram:
					hdps := m.Histogram().DataPoints()
					for d := 0; d < hdps.Len(); d++ {
						dp := hdps.At(d)
						out = append(out, Obs{
							ResolveID: id,
							SeriesKey: ingest.SeriesKey(resAttrs, scope.Name(), scope.Version(), scope.Attributes(), m.Name(), dp.Attributes()),
							Name:      m.Name(),
							Unit:      m.Unit(),
							Type:      "histogram",
							IsLimit:   cfg.IsLimit,
							Bounds:    dp.ExplicitBounds().AsRaw(),
							Counts:    dp.BucketCounts().AsRaw(),
							TsMs:      int64(dp.Timestamp()) / 1_000_000,
							Labels:    attrsToMap(dp.Attributes()),
						})
					}
				}
			}
		}
	}
	return out, nil
}

func numberDPs(m pmetric.Metric) (pmetric.NumberDataPointSlice, string, bool) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints(), "gauge", true
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints(), "sum", true
	}
	return pmetric.NumberDataPointSlice{}, "", false
}

func attrsToMap(m pcommon.Map) map[string]string {
	if m.Len() == 0 {
		return nil
	}
	out := make(map[string]string, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		out[k] = v.AsString()
		return true
	})
	return out
}
