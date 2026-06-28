// SPDX-License-Identifier: Apache-2.0

// Package langx is the shared extraction seam for web-language/runtime metrics:
// it decodes one OTLP export and produces the (id, signal, value, tsMs)
// observations that both the writer (store) and the analyzer (detect) consume,
// so the stored series and the detected series can never diverge.
package langx

import (
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
	"github.com/arrca-ai/arrca-metrics-commons/langreg"
)

// SeriesObs is one folded, scaled, rate-adjusted time-series observation.
type SeriesObs struct {
	ID, Key, Unit, Source string
	Counter               bool
	Value                 float64
	TsMs                  int64
}

// StaticObs is one folded, scaled static observation (string-formatted value).
type StaticObs struct {
	ID, Key, Unit, ValStr string
	TsMs                  int64
}

// Extract decodes one OTLP export. exist filters orphans; rates holds
// per-process counter→rate state (callers own the state, logic is shared).
func Extract(data []byte, exist *ingest.ExistenceSet, rates *ingest.RateTracker, now time.Time) ([]SeriesObs, []StaticObs, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(data); err != nil {
		return nil, nil, err
	}
	var series []SeriesObs
	var statics []StaticObs
	rms := req.Metrics().ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		getRes := ingest.AttrGetter(rm.Resource().Attributes())
		id, ok := ingest.ResolveID(ingest.LevelContainer, getRes)
		if !ok || !exist.Has(id) {
			continue
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				spec, ok := langreg.Lookup(m.Name())
				if !ok {
					continue
				}
				samples := numberSamples(m)
				if len(samples) == 0 {
					continue
				}
				for _, f := range spec.Fold(samples) {
					val := f.Value * spec.EffScale()
					if spec.Kind == langreg.KindStatic {
						statics = append(statics, StaticObs{
							ID: id, Key: f.Key, Unit: spec.Unit,
							ValStr: strconv.FormatFloat(val, 'f', -1, 64), TsMs: f.TsMs,
						})
						continue
					}
					if spec.Counter {
						rate, ok := rates.Rate(id+"|"+f.Key, val, f.TsMs, now)
						if !ok {
							continue
						}
						val = rate
					}
					series = append(series, SeriesObs{
						ID: id, Key: f.Key, Unit: spec.Unit, Source: spec.Source,
						Counter: spec.Counter, Value: val, TsMs: f.TsMs,
					})
				}
			}
		}
	}
	return series, statics, nil
}

// numberSamples extracts gauge/sum datapoints as []langreg.Sample.
func numberSamples(m pmetric.Metric) []langreg.Sample {
	var dps pmetric.NumberDataPointSlice
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps = m.Gauge().DataPoints()
	case pmetric.MetricTypeSum:
		dps = m.Sum().DataPoints()
	default:
		return nil
	}
	out := make([]langreg.Sample, 0, dps.Len())
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		v, ok := ingest.NumberValue(dp)
		if !ok {
			continue
		}
		attrs := map[string]string{}
		dp.Attributes().Range(func(k string, val pcommon.Value) bool {
			attrs[k] = val.AsString()
			return true
		})
		out = append(out, langreg.Sample{Attrs: attrs, Value: v, TsMs: int64(dp.Timestamp()) / 1_000_000})
	}
	return out
}
