// SPDX-License-Identifier: Apache-2.0

// Package redsig is the wire codec for derived RED signal samples published by
// graph-red and consumed by red-metric-analysis. The sample is exactly the
// anomaly Observe input minus the constant source ("red"), so the analyzer
// detects on the same values graph-red computed — drift is impossible.
package redsig

import "encoding/json"

// Sample is one derived RED signal observation.
type Sample struct {
	Signal   string  `json:"signal"`   // request_rate | error_rate | p99_latency
	ID       string  `json:"id"`
	Endpoint string  `json:"endpoint"` // epHash
	Value    float64 `json:"value"`
	TsMs     int64   `json:"tsMs"`
	Count    uint64  `json:"count"`
}

// Marshal encodes a batch of samples for one NATS message.
func Marshal(samples []Sample) ([]byte, error) { return json.Marshal(samples) }

// Unmarshal decodes a batch produced by Marshal.
func Unmarshal(data []byte) ([]Sample, error) {
	var s []Sample
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return s, nil
}
