// SPDX-License-Identifier: Apache-2.0
package events

import "strconv"

// g:anom stream field names. These are the single source of truth shared by the
// aggregator (writer) and graph-read (reader); both go through Encode/DecodeRow.
const (
	fTs         = "ts"
	fSource     = "source"
	fSignal     = "signal"
	fEndpoint   = "endpoint"
	fState      = "state"
	fDir        = "dir"
	fBaseline   = "baseline"
	fCurrent    = "current"
	fDeltaAbs   = "delta_abs"
	fDeltaRatio = "delta_ratio"
	fUnit       = "unit"
	fSinceMs    = "since_ms"
	fIncident   = "incident"
	fDesc       = "desc"
	fSeverity   = "severity"
	fOld        = "old"
	fNew        = "new"
	fReason     = "reason"
	fContainer  = "container"
)

// EncodeRow returns the XADD field/value pairs for an event (Desc assumed already
// rendered by the caller). Optional k8s/flip fields are omitted when empty —
// identical to the previous anomaly.Write behavior.
func EncodeRow(e Event) []any {
	vals := []any{
		fTs, strconv.FormatInt(e.TsMs, 10),
		fSource, e.Source,
		fSignal, e.Signal,
		fEndpoint, e.Endpoint,
		fState, e.State,
		fDir, e.Direction,
		fBaseline, ftoa(e.Baseline),
		fCurrent, ftoa(e.Current),
		fDeltaAbs, ftoa(e.DeltaAbs),
		fDeltaRatio, ftoa(e.DeltaRatio),
		fUnit, e.Unit,
		fSinceMs, strconv.FormatInt(e.SinceMs, 10),
		fIncident, e.IncidentID,
		fDesc, e.Desc,
		fSeverity, e.Severity,
	}
	if e.Old != "" {
		vals = append(vals, fOld, e.Old)
	}
	if e.New != "" {
		vals = append(vals, fNew, e.New)
	}
	if e.Reason != "" {
		vals = append(vals, fReason, e.Reason)
	}
	if e.Container != "" {
		vals = append(vals, fContainer, e.Container)
	}
	return vals
}

// DecodeRow rebuilds an Event from a stream entry's field map (the EntityID is the
// stream key, not a field, so callers fill it from the queried id if needed).
func DecodeRow(v map[string]interface{}) Event {
	get := func(k string) string {
		if s, ok := v[k].(string); ok {
			return s
		}
		return ""
	}
	return Event{
		Source:     get(fSource),
		Signal:     get(fSignal),
		Endpoint:   get(fEndpoint),
		State:      get(fState),
		Direction:  get(fDir),
		Baseline:   atof(get(fBaseline)),
		Current:    atof(get(fCurrent)),
		DeltaAbs:   atof(get(fDeltaAbs)),
		DeltaRatio: atof(get(fDeltaRatio)),
		Unit:       get(fUnit),
		TsMs:       atoi(get(fTs)),
		SinceMs:    atoi(get(fSinceMs)),
		IncidentID: get(fIncident),
		Old:        get(fOld),
		New:        get(fNew),
		Reason:     get(fReason),
		Container:  get(fContainer),
		Desc:       get(fDesc),
		Severity:   get(fSeverity),
	}
}

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
func atoi(s string) int64   { n, _ := strconv.ParseInt(s, 10, 64); return n }
func atof(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
