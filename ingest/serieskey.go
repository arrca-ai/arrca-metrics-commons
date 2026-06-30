// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"bytes"
	"hash/fnv"
	"sort"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Separators from the ASCII control range so they cannot appear in attribute
// text, keeping the canonical encoding unambiguous (mirrors otel-nats-hub).
const (
	skSepField  = 0x1f
	skSepRecord = 0x1e
)

// SeriesKey returns a deterministic, attribute-order-independent OPAQUE token
// identifying a datapoint's time series: resource attrs, scope (name/version/
// attrs), metric name, and datapoint attrs, hashed with FNV-1a. The decode map
// (g:cmn:meta) is the only way back to the components.
func SeriesKey(res pcommon.Map, scopeName, scopeVersion string, scopeAttrs pcommon.Map, metricName string, dp pcommon.Map) string {
	var b bytes.Buffer
	writeCanonicalMap(&b, res)
	b.WriteByte(skSepRecord)
	b.WriteString(scopeName)
	b.WriteByte(skSepField)
	b.WriteString(scopeVersion)
	b.WriteByte(skSepRecord)
	writeCanonicalMap(&b, scopeAttrs)
	b.WriteByte(skSepRecord)
	b.WriteString(metricName)
	b.WriteByte(skSepRecord)
	writeCanonicalMap(&b, dp)

	h := fnv.New64a()
	_, _ = h.Write(b.Bytes())
	return strconv.FormatUint(h.Sum64(), 16)
}

// writeCanonicalMap writes m's attributes sorted by key as
// "key<FS>type<FS>value<RS>" runs.
func writeCanonicalMap(b *bytes.Buffer, m pcommon.Map) {
	keys := make([]string, 0, m.Len())
	m.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	for _, k := range keys {
		v, _ := m.Get(k)
		b.WriteString(k)
		b.WriteByte(skSepField)
		b.WriteString(v.Type().String())
		b.WriteByte(skSepField)
		b.WriteString(v.AsString())
		b.WriteByte(skSepRecord)
	}
}
