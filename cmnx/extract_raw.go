// SPDX-License-Identifier: Apache-2.0

package cmnx

import (
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/cmnreg"
	"github.com/arrca-ai/arrca-metrics-commons/ingest"
	"github.com/arrca-ai/arrca-metrics-commons/labels"
)

// RawObs is one datapoint's pre-rate observation: the raw (unscaled) cumulative
// or instantaneous value plus the scale the caller should apply. Counters are
// left as raw cumulative values so the caller can compute deltas; Scale is
// applied to the delta (counters) or the value (gauges) by the caller.
type RawObs struct {
	ID, Key, Unit, Source string
	Counter               bool
	Raw, Scale            float64
	TsMs                  int64
	Labels                map[string]string
}

// ExtractRaw decodes one OTLP export into per-datapoint raw observations and
// limit values. Unlike Extract it performs NO rate conversion and keeps no
// state: the windowed-rollup writer owns delta computation. Extract is left
// untouched for the standalone analyzer.
func ExtractRaw(data []byte, exist *ingest.ExistenceSet) ([]RawObs, []LimitObs, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(data); err != nil {
		return nil, nil, err
	}
	var raw []RawObs
	var limits []LimitObs
	rms := req.Metrics().ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		getRes := ingest.AttrGetter(rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				spec, ok := cmnreg.Lookup(m.Name())
				if !ok {
					continue
				}
				id, ok := ingest.ResolveID(spec.Level, getRes)
				if !ok || !exist.Has(id) {
					continue
				}
				if spec.Limit {
					limits = appendLimits(limits, spec, id, m)
				} else {
					raw = appendRaw(raw, spec, id, m)
				}
			}
		}
	}
	return raw, limits, nil
}

func appendRaw(out []RawObs, spec cmnreg.MetricSpec, id string, m pmetric.Metric) []RawObs {
	dps, ok := numberDPs(m) // reused from extract.go (same package)
	if !ok {
		return out
	}
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		getAttr := ingest.AttrGetter(dp.Attributes())
		base, ok := spec.ResolveKey(getAttr)
		if !ok {
			continue
		}
		lbls, ok := spec.IdentityLabels(getAttr)
		if !ok {
			continue
		}
		key := labels.EncodeKey(base, lbls)
		v, ok := ingest.NumberValue(dp)
		if !ok {
			continue
		}
		out = append(out, RawObs{
			ID: id, Key: key, Unit: spec.Unit, Source: spec.Source,
			Counter: spec.Counter, Raw: v, Scale: spec.EffScale(),
			TsMs: int64(dp.Timestamp()) / 1_000_000, Labels: lbls,
		})
	}
	return out
}
