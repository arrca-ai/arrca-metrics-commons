// SPDX-License-Identifier: Apache-2.0
package kafkareg

import "sort"

// Kind classifies a Kafka metric's treatment.
type Kind int

const (
	KindSeries Kind = iota // capped stream + EWMA anomaly signal
	KindStatic             // summary hash field + flip point event
)

// Dim is a metric's breakdown granularity: the OTel Java agent emits each metric
// at exactly one attribute cardinality level (it records the most granular set).
type Dim int

const (
	DimNone           Dim = iota // client-wide: no topic/partition key (sentinel "_all")
	DimTopic                     // keyed by topic
	DimTopicPartition            // keyed by topic + partition
)

// Datapoint attribute keys (plain OTel Java agent names, not messaging.kafka.*).
const (
	AttrTopic     = "topic"
	AttrPartition = "partition"
)

// Spec declares how one OTLP Kafka metric is stored and analysed.
type Spec struct {
	Key    string // stream key (series) or summary field (static)
	Unit   string // render-ready unit
	Role   string // "consumer" | "producer"
	Kind   Kind
	Signal string // anomaly signal name (series only; "" for static)
	Dim    Dim    // breakdown granularity (DimNone for static + client-wide series)
}

// registry IS the demux: a metric name absent here is dropped. Only the
// high-leverage container-level client metrics are kept (see design spec).
var registry = map[string]Spec{
	// Time-series → stream + EWMA.
	"kafka.consumer.records_lag_max":         {Key: "lag", Unit: "records", Role: "consumer", Kind: KindSeries, Signal: "lag", Dim: DimTopicPartition},
	"kafka.consumer.records_consumed_rate":   {Key: "consumed_rate", Unit: "rec/s", Role: "consumer", Kind: KindSeries, Signal: "consumed_rate", Dim: DimTopic},
	"kafka.producer.record_send_rate":        {Key: "send_rate", Unit: "rec/s", Role: "producer", Kind: KindSeries, Signal: "send_rate", Dim: DimTopic},
	"kafka.producer.record_error_rate":       {Key: "error_rate", Unit: "err/s", Role: "producer", Kind: KindSeries, Signal: "error_rate", Dim: DimTopic},
	"kafka.producer.record_retry_rate":       {Key: "retry_rate", Unit: "ret/s", Role: "producer", Kind: KindSeries, Signal: "retry_rate", Dim: DimTopic},
	"kafka.producer.request_latency_avg":     {Key: "produce_latency", Unit: "ms", Role: "producer", Kind: KindSeries, Signal: "produce_latency"},
	"kafka.consumer.fetch_latency_avg":       {Key: "fetch_latency", Unit: "ms", Role: "consumer", Kind: KindSeries, Signal: "fetch_latency"},
	"kafka.consumer.commit_latency_avg":      {Key: "commit_latency", Unit: "ms", Role: "consumer", Kind: KindSeries, Signal: "commit_latency"},
	"kafka.consumer.rebalance_rate_per_hour": {Key: "rebalance_rate", Unit: "1/h", Role: "consumer", Kind: KindSeries, Signal: "rebalance_rate"},
	"kafka.producer.buffer_available_bytes":  {Key: "buffer_avail", Unit: "B", Role: "producer", Kind: KindSeries, Signal: "buffer_avail"},
	// Static → summary field + flip event (all client-wide).
	"kafka.consumer.assigned_partitions": {Key: "assigned_partitions", Role: "consumer", Kind: KindStatic},
	"kafka.consumer.connection_count":    {Key: "consumer_connections", Role: "consumer", Kind: KindStatic},
	"kafka.producer.connection_count":    {Key: "producer_connections", Role: "producer", Kind: KindStatic},
}

// Lookup returns the spec for an OTLP metric name; ok=false → drop.
func Lookup(name string) (Spec, bool) {
	s, ok := registry[name]
	return s, ok
}

// SeriesSpecs returns every time-series spec, ordered by Key (stable for the reader).
func SeriesSpecs() []Spec { return specsOfKind(KindSeries) }

// StaticSpecs returns every static spec, ordered by Key.
func StaticSpecs() []Spec { return specsOfKind(KindStatic) }

func specsOfKind(k Kind) []Spec {
	out := make([]Spec, 0, len(registry))
	for _, s := range registry {
		if s.Kind == k {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
