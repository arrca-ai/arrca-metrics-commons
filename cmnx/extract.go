// SPDX-License-Identifier: Apache-2.0

// Package cmnx is the shared extraction seam for cpu/mem/net (cmn) metrics: it
// decodes one OTLP export and produces the regular series observations AND the
// limit values both the writer (store) and analyzer (detect) consume.
package cmnx

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/cmnreg"
	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

type SeriesObs struct {
	ID, Key, Unit, Source string
	Counter               bool
	Value                 float64
	TsMs                  int64
}

type LimitObs struct {
	ID, Key string
	Value   float64
}

func Extract(data []byte, exist *ingest.ExistenceSet, rates *ingest.RateTracker, now time.Time) ([]SeriesObs, []LimitObs, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(data); err != nil {
		return nil, nil, err
	}
	var series []SeriesObs
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
					series = appendSeries(series, spec, id, m, rates, now)
				}
			}
		}
	}
	return series, limits, nil
}

func appendSeries(out []SeriesObs, spec cmnreg.MetricSpec, id string, m pmetric.Metric, rates *ingest.RateTracker, now time.Time) []SeriesObs {
	dps, ok := numberDPs(m)
	if !ok {
		return out
	}
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		key, ok := spec.ResolveKey(ingest.AttrGetter(dp.Attributes()))
		if !ok {
			continue
		}
		raw, ok := ingest.NumberValue(dp)
		if !ok {
			continue
		}
		tMs := int64(dp.Timestamp()) / 1_000_000
		val := raw
		if spec.Counter {
			rate, ok := rates.Rate(id+"|"+key, raw, tMs, now)
			if !ok {
				continue
			}
			val = rate
		}
		val *= spec.EffScale()
		out = append(out, SeriesObs{ID: id, Key: key, Unit: spec.Unit, Source: spec.Source, Counter: spec.Counter, Value: val, TsMs: tMs})
	}
	return out
}

func appendLimits(out []LimitObs, spec cmnreg.MetricSpec, id string, m pmetric.Metric) []LimitObs {
	dps, ok := numberDPs(m)
	if !ok {
		return out
	}
	for i := 0; i < dps.Len(); i++ {
		raw, ok := ingest.NumberValue(dps.At(i))
		if !ok {
			continue
		}
		out = append(out, LimitObs{ID: id, Key: spec.Key, Value: raw * spec.EffScale()})
	}
	return out
}

func numberDPs(m pmetric.Metric) (pmetric.NumberDataPointSlice, bool) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints(), true
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints(), true
	}
	return pmetric.NumberDataPointSlice{}, false
}
