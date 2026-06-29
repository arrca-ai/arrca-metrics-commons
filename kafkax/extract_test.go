// SPDX-License-Identifier: Apache-2.0
package kafkax

import (
	"context"
	"hash/fnv"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/arrca-ai/arrca-metrics-commons/ingest"
)

const testID = "container:shop/app-1/app"

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

// addContainerResource sets the container resource attributes matching testID.
func addContainerResource(ra pcommon.Map) {
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", "app-1")
	ra.PutStr("k8s.container.name", "app")
}

// gauge appends a single-datapoint gauge metric named `name` with value `v` and
// the given datapoint attributes (key/value pairs).
func gauge(sm pmetric.ScopeMetrics, name string, v float64, tMs int64, attrs ...string) {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(v)
	dp.SetTimestamp(tsNano(tMs))
	for i := 0; i+1 < len(attrs); i += 2 {
		dp.Attributes().PutStr(attrs[i], attrs[i+1])
	}
}

// buildExport mirrors buildExport in receiver_test.go:
// - kafka.consumer.records_lag_max (KindSeries, DimTopicPartition) topic=orders partition=0
// - kafka.consumer.assigned_partitions (KindStatic)
// - kafka.consumer.io_ratio (noise → dropped)
func buildExport(lag, assigned float64, tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	gauge(sm, "kafka.consumer.records_lag_max", lag, tMs, "topic", "orders", "partition", "0")
	gauge(sm, "kafka.consumer.assigned_partitions", assigned, tMs)
	gauge(sm, "kafka.consumer.io_ratio", 0.5, tMs) // noise → dropped
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

func TestExtract_SeriesAndStatic(t *testing.T) {
	exist := newTestExistence(t, testID)
	data := buildExport(100, 4, 1000)

	series, statics, err := Extract(data, exist)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	// --- Series: lag metric with DimTopicPartition ---
	if len(series) != 1 {
		t.Fatalf("want 1 series obs, got %d: %+v", len(series), series)
	}
	s := series[0]
	if s.ID != testID {
		t.Errorf("ID = %q, want %q", s.ID, testID)
	}
	if s.Key != "lag" {
		t.Errorf("Key = %q, want lag", s.Key)
	}
	if s.Unit != "records" {
		t.Errorf("Unit = %q, want records", s.Unit)
	}
	if s.Role != "consumer" {
		t.Errorf("Role = %q, want consumer", s.Role)
	}
	if s.Signal != "lag" {
		t.Errorf("Signal = %q, want lag", s.Signal)
	}
	if s.Topic != "orders" {
		t.Errorf("Topic = %q, want orders", s.Topic)
	}
	if s.Partition != "0" {
		t.Errorf("Partition = %q, want 0", s.Partition)
	}
	if s.DimLabel != "orders/0" {
		t.Errorf("DimLabel = %q, want orders/0", s.DimLabel)
	}
	if s.Value != 100 {
		t.Errorf("Value = %v, want 100", s.Value)
	}
	if s.TsMs != 1000 {
		t.Errorf("TsMs = %d, want 1000", s.TsMs)
	}

	// --- Static: assigned_partitions ---
	if len(statics) != 1 {
		t.Fatalf("want 1 static obs, got %d: %+v", len(statics), statics)
	}
	st := statics[0]
	if st.ID != testID {
		t.Errorf("static ID = %q, want %q", st.ID, testID)
	}
	if st.Role != "consumer" {
		t.Errorf("static Role = %q, want consumer", st.Role)
	}
	if st.Field != "assigned_partitions" {
		t.Errorf("static Field = %q, want assigned_partitions", st.Field)
	}
	if st.ValStr != "4" {
		t.Errorf("static ValStr = %q, want 4", st.ValStr)
	}
	if st.TsMs != 1000 {
		t.Errorf("static TsMs = %d, want 1000", st.TsMs)
	}
}

func TestExtract_DimTopic(t *testing.T) {
	exist := newTestExistence(t, testID)

	// kafka.consumer.records_consumed_rate is DimTopic (no partition attr required)
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	gauge(sm, "kafka.consumer.records_consumed_rate", 50, 2000, "topic", "payments")
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	data, _ := req.MarshalProto()

	series, statics, err := Extract(data, exist)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(statics) != 0 {
		t.Errorf("want no statics, got %d", len(statics))
	}
	if len(series) != 1 {
		t.Fatalf("want 1 series, got %d: %+v", len(series), series)
	}
	s := series[0]
	if s.Topic != "payments" {
		t.Errorf("Topic = %q, want payments", s.Topic)
	}
	if s.Partition != "" {
		t.Errorf("Partition = %q, want empty", s.Partition)
	}
	if s.DimLabel != "payments" {
		t.Errorf("DimLabel = %q, want payments", s.DimLabel)
	}
	if s.Signal != "consumed_rate" {
		t.Errorf("Signal = %q, want consumed_rate", s.Signal)
	}
	if s.Value != 50 {
		t.Errorf("Value = %v, want 50", s.Value)
	}
}

func TestExtract_DimNone(t *testing.T) {
	exist := newTestExistence(t, testID)

	// kafka.producer.request_latency_avg is DimNone (client-wide, no topic attr)
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	gauge(sm, "kafka.producer.request_latency_avg", 12.5, 3000)
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	data, _ := req.MarshalProto()

	series, _, err := Extract(data, exist)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("want 1 series, got %d", len(series))
	}
	s := series[0]
	if s.Topic != "" || s.Partition != "" {
		t.Errorf("DimNone must have empty topic/partition, got topic=%q partition=%q", s.Topic, s.Partition)
	}
	if s.DimLabel != "" {
		t.Errorf("DimLabel = %q, want empty for DimNone", s.DimLabel)
	}
	if s.Value != 12.5 {
		t.Errorf("Value = %v, want 12.5", s.Value)
	}
}

func TestExtract_RequiredAttrMissing_Dropped(t *testing.T) {
	exist := newTestExistence(t, testID)

	// kafka.consumer.records_lag_max requires topic AND partition; omit partition → drop.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	gauge(sm, "kafka.consumer.records_lag_max", 5, 1000, "topic", "orders") // no partition
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	data, _ := req.MarshalProto()

	series, statics, err := Extract(data, exist)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(series) != 0 || len(statics) != 0 {
		t.Fatalf("missing required dim attr must drop: series=%d statics=%d", len(series), len(statics))
	}
}

func TestExtract_UnknownEntityDropped(t *testing.T) {
	exist := newTestExistence(t, testID)

	// Build export for entity NOT in existence set
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", "ghost-1")
	ra.PutStr("k8s.container.name", "app")
	sm := rm.ScopeMetrics().AppendEmpty()
	gauge(sm, "kafka.consumer.records_lag_max", 100, 1000, "topic", "orders", "partition", "0")
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	data, _ := req.MarshalProto()

	series, statics, err := Extract(data, exist)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(series) != 0 || len(statics) != 0 {
		t.Fatalf("orphan entity must be dropped, got series=%d statics=%d", len(series), len(statics))
	}
}

func TestExtract_UnknownMetricDropped(t *testing.T) {
	exist := newTestExistence(t, testID)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	gauge(sm, "kafka.consumer.io_ratio", 0.5, 1000) // not in kafkareg
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	data, _ := req.MarshalProto()

	series, statics, err := Extract(data, exist)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(series) != 0 || len(statics) != 0 {
		t.Fatalf("unknown metric must be dropped, got series=%d statics=%d", len(series), len(statics))
	}
}

func TestExtract_MalformedProtoError(t *testing.T) {
	exist := newTestExistence(t, testID)
	_, _, err := Extract([]byte("not-proto"), exist)
	if err == nil {
		t.Fatal("malformed proto must return error")
	}
}

func TestFlipID_Determinism(t *testing.T) {
	// FlipID must be deterministic for the same inputs.
	a := FlipID("container:shop/app-1/app", "assigned_partitions", 1000)
	b := FlipID("container:shop/app-1/app", "assigned_partitions", 1000)
	if a != b {
		t.Errorf("FlipID not deterministic: %q != %q", a, b)
	}

	// Different tsMs must produce different ids.
	c := FlipID("container:shop/app-1/app", "assigned_partitions", 2000)
	if a == c {
		t.Errorf("FlipID must differ for different tsMs: both %q", a)
	}

	// Different field must produce different ids.
	d := FlipID("container:shop/app-1/app", "consumer_connections", 1000)
	if a == d {
		t.Errorf("FlipID must differ for different field: both %q", a)
	}
}

func TestFlipID_Format(t *testing.T) {
	// FlipID must be exactly 8 lowercase hex digits.
	id := FlipID("container:shop/app-1/app", "assigned_partitions", 1000)
	if len(id) != 8 {
		t.Errorf("FlipID len = %d, want 8 (got %q)", len(id), id)
	}
	for _, c := range id {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("FlipID contains non-hex char %q in %q", c, id)
		}
	}
}

// referenceFlipID is the verbatim algorithm from internal/kafka/receiver.go flipID,
// inlined here to prove byte-for-byte parity. If FlipID ever diverges, this fails.
func referenceFlipID(id, field string, tsMs int64) string {
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

func TestFlipID_ParityWithReceiverFlipID(t *testing.T) {
	// Parity test: FlipID must produce byte-identical output to internal/kafka flipID.
	// We use the same test vectors as receiver_test.go flip scenario:
	// id="container:shop/app-1/app", field="assigned_partitions", tsMs=2000
	vectors := []struct {
		id, field string
		tsMs      int64
	}{
		{"container:shop/app-1/app", "assigned_partitions", 2000},
		{"container:shop/app-1/app", "assigned_partitions", 1000},
		{"container:ns/pod/c", "consumer_connections", 999},
		{"container:ns/pod/c", "producer_connections", 0},
	}
	for _, v := range vectors {
		got := FlipID(v.id, v.field, v.tsMs)
		want := referenceFlipID(v.id, v.field, v.tsMs)
		if got != want {
			t.Errorf("FlipID(%q,%q,%d) = %q, want %q", v.id, v.field, v.tsMs, got, want)
		}
	}
}
