// SPDX-License-Identifier: Apache-2.0

// Package kafkax is the shared extraction seam for Kafka-client metrics:
// it decodes one OTLP export and produces the (id, signal, value, tsMs)
// observations that both the writer (store) and the analyzer (detect) consume,
// so the stored series and the detected series can never diverge.
//
// kafka applies no rate and no scale — value is the raw datapoint.
package kafkax

import (
	"hash/fnv"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
	"github.com/arrca-ai/arrca-metrics-commons/kafkareg"
)

// SeriesObs is one kafka time-series observation (value is the raw datapoint —
// kafka applies no rate/scale). Topic/Partition are for the writer's stream/meta
// keys; DimLabel is the anomaly endpoint for the analyzer.
type SeriesObs struct {
	ID, Key, Unit, Role, Signal string
	Topic, Partition, DimLabel  string
	Value                       float64
	TsMs                        int64
}

// StaticObs is one client-wide static datapoint (summary field + flip event).
type StaticObs struct {
	ID, Role, Field, ValStr string
	TsMs                    int64
}

// Extract decodes one OTLP export and returns the series and static observations.
// exist filters orphan entities; kafka has no rate or scale so the raw datapoint
// value is used directly.
func Extract(data []byte, exist *ingest.ExistenceSet) ([]SeriesObs, []StaticObs, error) {
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
				spec, ok := kafkareg.Lookup(m.Name())
				if !ok {
					continue
				}
				dps, ok := gaugePoints(m)
				if !ok {
					continue
				}
				for di := 0; di < dps.Len(); di++ {
					dp := dps.At(di)
					val, ok := ingest.NumberValue(dp)
					if !ok {
						continue
					}
					tMs := int64(dp.Timestamp()) / 1_000_000
					if spec.Kind == kafkareg.KindSeries {
						topic, partition, ok := resolveDim(dp.Attributes(), spec.Dim)
						if !ok {
							continue // required topic/partition attr missing → drop
						}
						series = append(series, SeriesObs{
							ID: id, Key: spec.Key, Unit: spec.Unit, Role: spec.Role, Signal: spec.Signal,
							Topic: topic, Partition: partition, DimLabel: dimLabel(topic, partition),
							Value: val, TsMs: tMs,
						})
					} else {
						statics = append(statics, StaticObs{
							ID: id, Role: spec.Role, Field: spec.Key,
							ValStr: strconv.FormatFloat(val, 'f', -1, 64), TsMs: tMs,
						})
					}
				}
			}
		}
	}
	return series, statics, nil
}

// FlipID is a stable 8-hex id for a static flip (entity, field, ts) — no wall
// clock. MUST match the value graph-kafka used so flip dedup is stable across
// the cutover. (Verbatim lift of internal/kafka flipID.)
func FlipID(id, field string, tsMs int64) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(field))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(tsMs, 10)))
	s := strconv.FormatUint(uint64(h.Sum32()), 16)
	for len(s) < 8 {
		s = "0" + s
	}
	return s
}

// resolveDim reads the (topic, partition) attributes a metric's Dim level
// requires. ok=false drops the datapoint when a required attr is absent.
// Verbatim lift from internal/kafka/receiver.go.
func resolveDim(attrs pcommon.Map, dim kafkareg.Dim) (topic, partition string, ok bool) {
	switch dim {
	case kafkareg.DimNone:
		return "", "", true
	case kafkareg.DimTopic:
		t, ok := attrs.Get(kafkareg.AttrTopic)
		if !ok || t.AsString() == "" {
			return "", "", false
		}
		return t.AsString(), "", true
	case kafkareg.DimTopicPartition:
		t, ok := attrs.Get(kafkareg.AttrTopic)
		if !ok || t.AsString() == "" {
			return "", "", false
		}
		p, ok := attrs.Get(kafkareg.AttrPartition)
		if !ok || p.AsString() == "" {
			return "", "", false
		}
		return t.AsString(), p.AsString(), true
	}
	return "", "", false
}

// dimLabel is the anomaly endpoint label for a dimension: "" (client-wide),
// "<topic>", or "<topic>/<partition>".
// Verbatim lift from internal/kafka/receiver.go.
func dimLabel(topic, partition string) string {
	switch {
	case topic == "":
		return ""
	case partition == "":
		return topic
	default:
		return topic + "/" + partition
	}
}

// gaugePoints returns the NumberDataPointSlice for Gauge and Sum metrics.
// Verbatim lift from internal/kafka/receiver.go.
func gaugePoints(m pmetric.Metric) (pmetric.NumberDataPointSlice, bool) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints(), true
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints(), true
	default:
		return pmetric.NumberDataPointSlice{}, false
	}
}
