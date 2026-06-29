// SPDX-License-Identifier: Apache-2.0
package events

import "testing"

func TestEventRoundTrip(t *testing.T) {
	in := Event{
		EntityID: "container:default/api-1/app", Source: "red", Signal: "p99_latency",
		Endpoint: "0b3f7a9c", State: StateOnset, Direction: DirUp,
		Baseline: 210, Current: 540, DeltaAbs: 330, DeltaRatio: 1.57,
		Unit: "ms", TsMs: 1782547098597, IncidentID: "ab12cd34",
	}
	b, err := in.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	out, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.EntityID != in.EntityID || out.Source != in.Source || out.Signal != in.Signal ||
		out.Endpoint != in.Endpoint || out.State != in.State || out.Direction != in.Direction ||
		out.Baseline != in.Baseline || out.Current != in.Current ||
		out.DeltaAbs != in.DeltaAbs || out.DeltaRatio != in.DeltaRatio ||
		out.Unit != in.Unit || out.TsMs != in.TsMs || out.IncidentID != in.IncidentID {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	if _, err := Unmarshal([]byte("not json")); err == nil {
		t.Fatal("expected error on garbage")
	}
}
