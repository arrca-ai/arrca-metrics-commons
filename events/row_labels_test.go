// SPDX-License-Identifier: Apache-2.0
package events

import (
	"reflect"
	"testing"
)

func TestRowLabelsRoundTrip(t *testing.T) {
	e := Event{
		EntityID: "container:default/auth-1/app", Source: "runtime",
		Signal: "jvm_nonheap_limit{pool=Metaspace}", State: StateEvent,
		Unit: "MB", TsMs: 1000, IncidentID: "abc", Old: "117", New: "1024",
		Labels: map[string]string{"pool": "Metaspace"},
	}
	row := EncodeRow(e)
	fields := map[string]interface{}{}
	for i := 0; i+1 < len(row); i += 2 {
		fields[row[i].(string)] = row[i+1]
	}
	got := DecodeRow(fields)
	if !reflect.DeepEqual(got.Labels, e.Labels) {
		t.Fatalf("labels did not round-trip: %+v", got.Labels)
	}
}
