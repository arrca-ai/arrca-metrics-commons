// SPDX-License-Identifier: Apache-2.0
package redsig

import (
	"reflect"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := []Sample{
		{Signal: "request_rate", ID: "c1", Endpoint: "ep1", Value: 12.5, TsMs: 1000, Count: 50},
		{Signal: "error_rate", ID: "c1", Endpoint: "ep1", Value: 0.04, TsMs: 1000, Count: 50},
		{Signal: "p99_latency", ID: "c1", Endpoint: "ep1", Value: 250.0, TsMs: 1000, Count: 50},
	}
	data, err := Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestUnmarshalGarbage(t *testing.T) {
	if _, err := Unmarshal([]byte("not json")); err == nil {
		t.Fatal("expected error on garbage input, got nil")
	}
}

func TestMarshalEmpty(t *testing.T) {
	data, err := Marshal(nil)
	if err != nil {
		t.Fatalf("Marshal(nil): %v", err)
	}
	out, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty, got %d", len(out))
	}
}
