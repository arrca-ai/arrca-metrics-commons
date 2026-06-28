// SPDX-License-Identifier: Apache-2.0
package kafkareg

import "testing"

func TestLookupClassifies(t *testing.T) {
	cases := []struct {
		name string
		key  string
		kind Kind
	}{
		{"kafka.consumer.records_lag_max", "lag", KindSeries},
		{"kafka.producer.record_error_rate", "error_rate", KindSeries},
		{"kafka.consumer.assigned_partitions", "assigned_partitions", KindStatic},
		{"kafka.producer.connection_count", "producer_connections", KindStatic},
	}
	for _, c := range cases {
		s, ok := Lookup(c.name)
		if !ok {
			t.Fatalf("%s not in registry", c.name)
		}
		if s.Key != c.key || s.Kind != c.kind {
			t.Fatalf("%s -> %+v, want key=%s kind=%v", c.name, s, c.key, c.kind)
		}
	}
	if _, ok := Lookup("kafka.consumer.io_ratio"); ok {
		t.Fatal("noise metric io_ratio must NOT be in registry")
	}
}

func TestSeriesAndStaticSpecsPartition(t *testing.T) {
	if len(SeriesSpecs()) != 10 {
		t.Fatalf("SeriesSpecs len = %d, want 10", len(SeriesSpecs()))
	}
	if len(StaticSpecs()) != 3 {
		t.Fatalf("StaticSpecs len = %d, want 3", len(StaticSpecs()))
	}
	for _, s := range SeriesSpecs() {
		if s.Signal == "" {
			t.Fatalf("series spec %q missing Signal", s.Key)
		}
	}
}

func TestLookupDimLevel(t *testing.T) {
	cases := []struct {
		name string
		dim  Dim
	}{
		{"kafka.consumer.records_lag_max", DimTopicPartition},
		{"kafka.consumer.records_consumed_rate", DimTopic},
		{"kafka.producer.record_send_rate", DimTopic},
		{"kafka.producer.record_error_rate", DimTopic},
		{"kafka.producer.record_retry_rate", DimTopic},
		{"kafka.producer.request_latency_avg", DimNone},
		{"kafka.consumer.fetch_latency_avg", DimNone},
		{"kafka.consumer.assigned_partitions", DimNone},
		{"kafka.producer.connection_count", DimNone},
	}
	for _, c := range cases {
		s, ok := Lookup(c.name)
		if !ok {
			t.Fatalf("%s not in registry", c.name)
		}
		if s.Dim != c.dim {
			t.Fatalf("%s dim = %v, want %v", c.name, s.Dim, c.dim)
		}
	}
}
