// SPDX-License-Identifier: Apache-2.0
package events

import "encoding/json"

// Event lifecycle states and directions.
const (
	StateOnset     = "onset"
	StateRecovered = "recovered"
	StateEvent     = "event" // point event (k8s transition); no onset/recovery
	DirUp          = "up"
	DirDown        = "down"
)

// Event is one anomaly transition. It travels JSON-encoded on NATS (Desc empty)
// and is read back from Redis with Desc populated by the aggregator.
type Event struct {
	EntityID   string  `json:"entityID"`
	Source     string  `json:"source"` // "metrics" | "red" | "k8s" | "kafka"
	Signal     string  `json:"signal"`
	State      string  `json:"state"`     // onset | recovered
	Direction  string  `json:"direction"` // up | down
	Baseline   float64 `json:"baseline"`
	Current    float64 `json:"current"`
	DeltaAbs   float64 `json:"deltaAbs"`
	DeltaRatio float64 `json:"deltaRatio"`
	Unit       string  `json:"unit"`
	TsMs       int64   `json:"tsMs"`
	SinceMs    int64   `json:"sinceMs,omitempty"` // incident duration; recovery only
	IncidentID string  `json:"incidentID"`
	// k8s point-event fields (source "k8s"); empty for metric/RED rows.
	Old       string `json:"old,omitempty"`       // prev phase / restart count / image
	New       string `json:"new,omitempty"`       // new phase / restart count / image
	Reason    string `json:"reason,omitempty"`    // termination reason (pod_restart)
	Container string `json:"container,omitempty"` // container name
	Desc      string `json:"desc,omitempty"`      // rendered verbose description
	Severity  string `json:"severity,omitempty"`  // "red" | "yellow"; stamped by the aggregator
	Labels    map[string]string `json:"labels,omitempty"` // identity labels (pool, topic, endpoint…); read by renderers
}

// Marshal encodes the event for the wire / stream.
func (e Event) Marshal() ([]byte, error) { return json.Marshal(e) }

// Unmarshal decodes an event from JSON.
func Unmarshal(b []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(b, &e)
	return e, err
}
