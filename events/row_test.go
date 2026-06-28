// SPDX-License-Identifier: Apache-2.0
package events

import "testing"

func TestEncodeDecodeRowRoundTrip(t *testing.T) {
	e := Event{
		Source: "runtime", Signal: "jvm_heap_used", Endpoint: "",
		State: StateOnset, Direction: DirUp,
		Baseline: 220, Current: 540, DeltaAbs: 320, DeltaRatio: 1.45,
		Unit: "MB", TsMs: 1782547098597, SinceMs: 0, IncidentID: "ab12cd34",
		Desc: "JVM heap used rose 220 MB -> 540 MB", Severity: "yellow",
	}
	// EncodeRow returns the XADD field/value pairs; simulate the stream readback
	// as the string-keyed map go-redis hands to DecodeRow.
	vals := EncodeRow(e)
	m := map[string]interface{}{}
	for i := 0; i+1 < len(vals); i += 2 {
		m[vals[i].(string)] = vals[i+1].(string)
	}
	got := DecodeRow(m)
	if got.Source != e.Source || got.Signal != e.Signal || got.State != e.State ||
		got.Direction != e.Direction || got.Unit != e.Unit || got.TsMs != e.TsMs ||
		got.IncidentID != e.IncidentID || got.Desc != e.Desc || got.Severity != e.Severity {
		t.Fatalf("string fields lost: %+v", got)
	}
	if got.Baseline != e.Baseline || got.Current != e.Current ||
		got.DeltaAbs != e.DeltaAbs || got.DeltaRatio != e.DeltaRatio {
		t.Fatalf("float fields lost: %+v", got)
	}
}

func TestEncodeRowOmitsEmptyOptionalFields(t *testing.T) {
	vals := EncodeRow(Event{Source: "metrics", Signal: "cpu", State: StateOnset})
	m := map[string]bool{}
	for i := 0; i+1 < len(vals); i += 2 {
		m[vals[i].(string)] = true
	}
	for _, k := range []string{"old", "new", "reason", "container"} {
		if m[k] {
			t.Fatalf("empty optional field %q must be omitted", k)
		}
	}
}

func TestDecodeRowK8sPointEvent(t *testing.T) {
	m := map[string]interface{}{
		"source": "k8s", "signal": "pod_restart", "state": StateEvent,
		"ts": "1782547000000", "incident": "9f2a1c40",
		"old": "3", "new": "4", "reason": "OOMKilled", "container": "app",
		"severity": "red", "desc": "container app restarted",
	}
	e := DecodeRow(m)
	if e.Old != "3" || e.New != "4" || e.Reason != "OOMKilled" || e.Container != "app" ||
		e.Severity != "red" || e.State != StateEvent {
		t.Fatalf("k8s fields wrong: %+v", e)
	}
}
