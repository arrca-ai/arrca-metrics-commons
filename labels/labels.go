// SPDX-License-Identifier: Apache-2.0

// Package labels is the identity-label primitive shared by every metric
// registry, the anomaly wire contract, and the stores: a series is identified by
// (entity, base key, Labels), and EncodeKey is the one canonical key codec.
package labels

import (
	"sort"
	"strings"
)

// Labels is an identity-bearing label set (label name → value).
type Labels = map[string]string

// LabelSpec declares one identity-bearing datapoint label. Values are discovered
// at runtime (not enumerated): Attr is the OTLP datapoint attribute, Name is the
// short label name used in keys and rendering.
type LabelSpec struct {
	Name string // short label name, e.g. "pool", "topic"
	Attr string // OTLP datapoint attribute, e.g. "jvm.memory.pool.name"
}

// EncodeKey builds the canonical key from a base key and a label set. No labels →
// base unchanged. With labels → base{n1=v1,n2=v2} (names sorted; deterministic).
func EncodeKey(base string, l Labels) string {
	if len(l) == 0 {
		return base
	}
	names := make([]string, 0, len(l))
	for n := range l {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(base)
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteByte('=')
		b.WriteString(l[n])
	}
	b.WriteByte('}')
	return b.String()
}

// DecodeKey splits an encoded key back into base + labels. A key with no "{...}"
// returns (key, nil). Label values contain no '{', '}', '=', ',' (enforced by
// the attributes we key on), so the split is unambiguous.
func DecodeKey(s string) (string, Labels) {
	i := strings.IndexByte(s, '{')
	if i < 0 || !strings.HasSuffix(s, "}") {
		return s, nil
	}
	base := s[:i]
	inner := s[i+1 : len(s)-1]
	if inner == "" {
		return base, nil
	}
	out := Labels{}
	for _, pair := range strings.Split(inner, ",") {
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}
		out[pair[:eq]] = pair[eq+1:]
	}
	return base, out
}
