// SPDX-License-Identifier: Apache-2.0

// Package otlpx is a stateless OTLP sum/gauge extraction seam: it decodes one
// OTLP export into per-datapoint observations keyed by an opaque SeriesKey,
// dropping any metric absent from the caller's Registry. It computes no rate and
// does no existence gating — callers layer those on top. Lifted from
// arrca-graph/internal/sumgauge so the detect side (metrics-analysis) can share it.
package otlpx

import "github.com/arrca-ai/arrca-metrics-commons/ingest"

// MetricConfig declares one accepted metric: which level resolves its entity id,
// and whether it is a limit value (vs. a regular series).
type MetricConfig struct {
	Name    string
	Level   ingest.Level
	IsLimit bool
}

// Registry is a name → config lookup. Code-defined; a name absent here is dropped.
type Registry map[string]MetricConfig

func NewRegistry(cfgs []MetricConfig) Registry {
	r := make(Registry, len(cfgs))
	for _, c := range cfgs {
		r[c.Name] = c
	}
	return r
}

func (r Registry) Lookup(name string) (MetricConfig, bool) { c, ok := r[name]; return c, ok }
