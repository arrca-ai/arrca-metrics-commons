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
