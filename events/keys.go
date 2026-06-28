// SPDX-License-Identifier: Apache-2.0

// Package events is the anomaly wire + g:anom Redis row contract shared by the aggregator (writer) and graph-read (reader).
package events

// ns is the anomaly Redis namespace, under graph's "g:" so it can never collide
// with arrca-flows' prefixes and is distinct from g:red / g:ts / g:meta.
const ns = "g:anom"

// IDsKey indexes every entity that currently has anomaly events.
func IDsKey() string { return ns + ":ids" }

// StreamKey is one entity's anomaly event stream.
func StreamKey(id string) string { return ns + ":s:" + id }
