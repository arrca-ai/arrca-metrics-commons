# Labels as Series Identity — Plan: Phase 0 (Core) + Phase 1A (Runtime extraction)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the unified label primitive in `arrca-metrics-commons` and make runtime extraction emit per-`(base key, label)` observations, so `jvm.memory.limit` produces one stable series per pool instead of a collapsed flapping key.

**Architecture:** A new leaf package `labels` holds the `Labels` type, `LabelSpec`, and the single `EncodeKey`/`DecodeKey` codec. `langreg` gains `Signal.Labels` and `Fold` groups datapoints by `(base, label set)` via `EncodeKey` (discovery model — labels declared by attribute, values NOT enumerated). `langx` carries the structured labels through `Extract`. `events.Event` gains `Labels` additively (the `Endpoint` field stays until Phase 1B retires it).

**Tech Stack:** Go 1.x, `go.opentelemetry.io/collector/pdata`, miniredis (tests). Single repo: `arrca-metrics-commons`.

**Scope:** Phase 0 + 1A only — all changes are in `arrca-metrics-commons` and leave it building+green and behaviorally unchanged for existing consumers (additive). **Phase 1B (the cross-repo consumer switch) is a separate plan**: emitter `Observe` endpoint→labels (metrics-analysis), graph-web-languages discovery index + graph-read discovery reader (arrca-graph), and dropping `events.Event.Endpoint`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-29-labels-as-series-identity-design.md` (unified, discovery-based).
- Discovery model: labels declared by **attribute only**, no enumerated `Values`. A declared label whose attr is absent on a datapoint → drop that datapoint.
- `EncodeKey(base, nil) == base` (legacy keys unchanged); one codec used everywhere.
- Phase 0/1A is **additive** — `go build ./... && go test ./...` in commons stays green; `events.Event.Endpoint` is NOT removed here.
- TDD: failing test first. Commit after each task. Commit messages end with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Run all commands from the `arrca-metrics-commons` repo root.

## File Structure

- Create: `labels/labels.go` — `Labels`, `LabelSpec`, `EncodeKey`, `DecodeKey`.
- Create: `labels/labels_test.go`.
- Modify: `events/event.go` — add `Event.Labels`.
- Modify: `events/row.go` — encode/decode `labels` field.
- Create: `events/row_labels_test.go`.
- Modify: `langreg/registry.go` — `Signal.Labels`, `Folded.Labels`, `Fold` regroup, `identityLabels`, `jvm.memory.limit` entry, `labels` import.
- Modify: `langreg/registry_test.go`.
- Modify: `langx/extract.go` — `SeriesObs.Labels`, `StaticObs.Labels`, carry-through.
- Modify: `langx/extract_test.go`.

---

### Task 1: `labels` package — `EncodeKey`/`DecodeKey` + types

**Files:**
- Create: `labels/labels.go`
- Test: `labels/labels_test.go`

**Interfaces:**
- Produces: `type Labels = map[string]string`; `type LabelSpec struct { Name, Attr string }`; `func EncodeKey(base string, l Labels) string`; `func DecodeKey(s string) (string, Labels)`.

- [ ] **Step 1: Write the failing test** — create `labels/labels_test.go`:

```go
// SPDX-License-Identifier: Apache-2.0
package labels

import (
	"reflect"
	"testing"
)

func TestEncodeKey(t *testing.T) {
	if got := EncodeKey("jvm_nonheap_limit", nil); got != "jvm_nonheap_limit" {
		t.Fatalf("nil labels must be base, got %q", got)
	}
	if got := EncodeKey("k", Labels{}); got != "k" {
		t.Fatalf("empty labels must be base, got %q", got)
	}
	if got := EncodeKey("lag", Labels{"topic": "orders", "partition": "3"}); got != "lag{partition=3,topic=orders}" {
		t.Fatalf("labels must be sorted by name: %q", got)
	}
}

func TestDecodeKey(t *testing.T) {
	base, l := DecodeKey("jvm_nonheap_limit")
	if base != "jvm_nonheap_limit" || len(l) != 0 {
		t.Fatalf("no-label decode wrong: %q %v", base, l)
	}
	base, l = DecodeKey("lag{partition=3,topic=orders}")
	if base != "lag" || !reflect.DeepEqual(l, Labels{"topic": "orders", "partition": "3"}) {
		t.Fatalf("decode wrong: %q %v", base, l)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Labels{"pool": "Compressed Class Space"}
	base, out := DecodeKey(EncodeKey("jvm_nonheap_limit", in))
	if base != "jvm_nonheap_limit" || !reflect.DeepEqual(out, in) {
		t.Fatalf("round-trip wrong: %q %v", base, out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./labels/`
Expected: FAIL — `no required module provides package .../labels` / undefined symbols.

- [ ] **Step 3: Create the package** — `labels/labels.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./labels/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add labels/labels.go labels/labels_test.go
git commit -m "feat(labels): identity-label primitive + EncodeKey/DecodeKey

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `events.Event.Labels` (additive) + row codec

**Files:**
- Modify: `events/event.go`, `events/row.go`
- Test: `events/row_labels_test.go`

**Interfaces:**
- Produces: `Event.Labels map[string]string` (json `labels,omitempty`); `EncodeRow`/`DecodeRow` round-trip it under the `labels` field. `Endpoint` is unchanged (retired in Phase 1B).

- [ ] **Step 1: Write the failing test** — create `events/row_labels_test.go`:

```go
// SPDX-License-Identifier: Apache-2.0
package events

import "testing"

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
	if got.Labels["pool"] != "Metaspace" {
		t.Fatalf("labels did not round-trip: %+v", got.Labels)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./events/ -run TestRowLabelsRoundTrip`
Expected: FAIL — `unknown field 'Labels' in struct literal of type Event`.

- [ ] **Step 3: Add the field + codec** — in `events/event.go`, add to the `Event` struct after `Severity`:

```go
	Labels map[string]string `json:"labels,omitempty"` // identity labels (pool, topic, endpoint…); read by renderers
```

In `events/row.go`: add `"encoding/json"` to imports; add `fLabels = "labels"` to the const block; in `EncodeRow`, before `return vals`:

```go
	if len(e.Labels) > 0 {
		if b, err := json.Marshal(e.Labels); err == nil {
			vals = append(vals, fLabels, string(b))
		}
	}
```

In `DecodeRow`, before the `return Event{...}`:

```go
	var labels map[string]string
	if s := get(fLabels); s != "" {
		_ = json.Unmarshal([]byte(s), &labels)
	}
```

and add `Labels: labels,` to the returned `Event{...}` literal.

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./events/`
Expected: PASS (existing row tests unaffected — `labels` is omitted when empty).

- [ ] **Step 5: Commit**

```bash
git add events/event.go events/row.go events/row_labels_test.go
git commit -m "feat(events): add Event.Labels + g:anom row carry (additive)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `langreg` — `Signal.Labels` + `Fold` groups by `(base, labels)`

**Files:**
- Modify: `langreg/registry.go`
- Test: `langreg/registry_test.go`

**Interfaces:**
- Consumes: `labels.EncodeKey`, `labels.LabelSpec` (Task 1).
- Produces: `Signal.Labels []labels.LabelSpec`; `Folded.Labels map[string]string`; `Fold` emits one `Folded` per `(base, label set)`; `func (s Signal) identityLabels(attrs map[string]string) (map[string]string, bool)`.

- [ ] **Step 1: Write the failing test** — add to `langreg/registry_test.go`:

```go
func TestFoldSplitsByLabel(t *testing.T) {
	s := Signal{
		Kind: KindStatic, FoldAttr: "jvm.memory.type",
		FoldMap: map[string]string{"non_heap": "jvm_nonheap_limit"},
		Labels:  []labels.LabelSpec{{Name: "pool", Attr: "jvm.memory.pool.name"}},
	}
	got := s.Fold([]Sample{
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "Metaspace"}, Value: 1024, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "Code Cache"}, Value: 117, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.type": "non_heap"}, Value: 9, TsMs: 5}, // missing pool attr → dropped
	})
	if len(got) != 2 {
		t.Fatalf("want 2 per-pool keys, got %d: %+v", len(got), got)
	}
	want := map[string]float64{
		"jvm_nonheap_limit{pool=Metaspace}":  1024,
		"jvm_nonheap_limit{pool=Code Cache}": 117,
	}
	for _, f := range got {
		if want[f.Key] != f.Value {
			t.Fatalf("key %s = %v, want %v", f.Key, f.Value, want[f.Key])
		}
		if f.Labels["pool"] == "" {
			t.Fatalf("Folded.Labels not populated: %+v", f)
		}
	}
}
```

Add the import to `langreg/registry_test.go` (the test file `package langreg` block): `"github.com/arrca-ai/arrca-metrics-commons/labels"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langreg/ -run TestFoldSplitsByLabel`
Expected: FAIL — `s.Labels undefined` / `unknown field 'Labels'`.

- [ ] **Step 3: Wire labels into the registry + Fold** — in `langreg/registry.go`:

Add the import `"github.com/arrca-ai/arrca-metrics-commons/labels"`.

Add to `Signal` (after `Source`):

```go
	Labels []labels.LabelSpec // identity-bearing labels (discovery model; values not enumerated)
```

Add to `Folded` (after `TsMs`):

```go
	Labels map[string]string // identity labels (nil when the signal declares none)
```

Replace the `Fold` method body with:

```go
func (s Signal) Fold(dps []Sample) []Folded {
	// Fast path: no fold attr and no identity labels → passthrough per datapoint.
	if s.FoldAttr == "" && len(s.Labels) == 0 {
		if s.Key == "" {
			return nil
		}
		out := make([]Folded, 0, len(dps))
		for _, d := range dps {
			out = append(out, Folded{Key: s.Key, Value: d.Value, TsMs: d.TsMs})
		}
		return out
	}
	type acc struct {
		lbls map[string]string
		sum  float64
		tsMs int64
	}
	m := map[string]*acc{}
	for _, d := range dps {
		base := s.Key
		if s.FoldAttr != "" {
			v, ok := d.Attrs[s.FoldAttr]
			if !ok {
				continue
			}
			base, ok = s.FoldMap[v]
			if !ok {
				continue
			}
		}
		if base == "" {
			continue
		}
		lbls, ok := s.identityLabels(d.Attrs)
		if !ok {
			continue // a declared identity label's attr is missing → drop datapoint
		}
		key := labels.EncodeKey(base, lbls)
		a := m[key]
		if a == nil {
			a = &acc{lbls: lbls}
			m[key] = a
		}
		a.sum += d.Value
		if d.TsMs > a.tsMs {
			a.tsMs = d.TsMs
		}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Folded, 0, len(keys))
	for _, k := range keys {
		out = append(out, Folded{Key: k, Labels: m[k].lbls, Value: m[k].sum, TsMs: m[k].tsMs})
	}
	return out
}

// identityLabels extracts the declared identity labels from a datapoint's attrs.
// ok=false if any declared label's attr is absent (caller drops the datapoint).
// Returns nil when the signal declares no labels.
func (s Signal) identityLabels(attrs map[string]string) (map[string]string, bool) {
	if len(s.Labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(s.Labels))
	for _, l := range s.Labels {
		v, ok := attrs[l.Attr]
		if !ok {
			return nil, false
		}
		out[l.Name] = v
	}
	return out, true
}
```

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./langreg/`
Expected: PASS — including existing `TestFoldSumsPoolsByType` (fold-no-labels still sums to one key per type) and `TestFoldNonFoldPassThrough` (fast path).

- [ ] **Step 5: Commit**

```bash
git add langreg/registry.go langreg/registry_test.go
git commit -m "feat(langreg): Signal.Labels + Fold groups by (base, labels)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: register per-pool labels on `jvm.memory.limit`

**Files:**
- Modify: `langreg/registry.go`
- Test: `langreg/registry_test.go`

**Interfaces:**
- Consumes: `Signal.Labels`, `Fold` (Task 3).

- [ ] **Step 1: Write the failing test** — add to `langreg/registry_test.go`:

```go
func TestMemoryLimitIsPerPool(t *testing.T) {
	s, ok := Lookup("jvm.memory.limit")
	if !ok || len(s.Labels) != 1 || s.Labels[0].Name != "pool" || s.Labels[0].Attr != "jvm.memory.pool.name" {
		t.Fatalf("jvm.memory.limit must declare a pool label: %+v", s)
	}
	got := s.Fold([]Sample{
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "Metaspace"}, Value: 1, TsMs: 1},
		{Attrs: map[string]string{"jvm.memory.type": "heap", "jvm.memory.pool.name": "G1 Old Gen"}, Value: 2, TsMs: 1},
	})
	keys := map[string]bool{}
	for _, f := range got {
		keys[f.Key] = true
	}
	if !keys["jvm_nonheap_limit{pool=Metaspace}"] || !keys["jvm_heap_limit{pool=G1 Old Gen}"] {
		t.Fatalf("per-pool keys missing: %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langreg/ -run TestMemoryLimitIsPerPool`
Expected: FAIL — `len(s.Labels) != 1`.

- [ ] **Step 3: Add `Labels` to the registry entry** — in `langreg/registry.go`, replace the `jvm.memory.limit` line with:

```go
	"jvm.memory.limit": {Unit: "MB", Kind: KindStatic, FoldAttr: "jvm.memory.type", FoldMap: map[string]string{"heap": "jvm_heap_limit", "non_heap": "jvm_nonheap_limit"}, Scale: bytesToMB, Source: "java", Labels: []labels.LabelSpec{{Name: "pool", Attr: "jvm.memory.pool.name"}}},
```

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./langreg/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add langreg/registry.go langreg/registry_test.go
git commit -m "feat(langreg): per-pool label on jvm.memory.limit

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: `langx` carries labels through `Extract`

**Files:**
- Modify: `langx/extract.go`
- Test: `langx/extract_test.go`

**Interfaces:**
- Consumes: `Folded.Labels` (Task 3), per-pool `jvm.memory.limit` (Task 4).
- Produces: `SeriesObs.Labels`, `StaticObs.Labels` (`map[string]string`).

- [ ] **Step 1: Write the failing test + helper** — add to `langx/extract_test.go`:

```go
func buildMultiPoolLimitExport(tMs int64) []byte {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	addContainerResource(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("jvm.memory.limit")
	dps := m.SetEmptyGauge().DataPoints()
	add := func(typ, pool string, v float64) {
		d := dps.AppendEmpty()
		d.SetDoubleValue(v)
		d.SetTimestamp(tsNano(tMs))
		d.Attributes().PutStr("jvm.memory.type", typ)
		d.Attributes().PutStr("jvm.memory.pool.name", pool)
	}
	add("non_heap", "Metaspace", 1024*1024*1024)
	add("non_heap", "Compressed Class Space", 5*1024*1024)
	add("heap", "G1 Old Gen", 170*1024*1024)
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	b, _ := req.MarshalProto()
	return b
}

func TestExtractPerPoolLimit(t *testing.T) {
	exist := newTestExistence(t, testID)
	_, statics, err := Extract(buildMultiPoolLimitExport(1000), exist, ingest.NewRateTracker(time.Minute), time.Unix(1, 0))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	byKey := map[string]StaticObs{}
	for _, s := range statics {
		byKey[s.Key] = s
	}
	if len(byKey) != 3 {
		t.Fatalf("want 3 per-pool statics, got %d: %+v", len(byKey), statics)
	}
	meta, ok := byKey["jvm_nonheap_limit{pool=Metaspace}"]
	if !ok || meta.Labels["pool"] != "Metaspace" {
		t.Fatalf("Metaspace static missing or unlabelled: %+v", statics)
	}
	if _, ok := byKey["jvm_heap_limit{pool=G1 Old Gen}"]; !ok {
		t.Fatalf("heap pool key missing: %+v", statics)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langx/ -run TestExtractPerPoolLimit`
Expected: FAIL — `unknown field 'Labels' in struct literal of type StaticObs`.

- [ ] **Step 3: Add `Labels` to the obs structs + carry it** — in `langx/extract.go`:

Replace the two struct definitions:

```go
// SeriesObs is one folded, scaled, rate-adjusted time-series observation.
type SeriesObs struct {
	ID, Key, Unit, Source string
	Counter               bool
	Value                 float64
	TsMs                  int64
	Labels                map[string]string
}

// StaticObs is one folded, scaled static observation (string-formatted value).
type StaticObs struct {
	ID, Key, Unit, ValStr string
	TsMs                  int64
	Labels                map[string]string
}
```

In `Extract`, set `Labels: f.Labels` in both append sites:

```go
		if spec.Kind == langreg.KindStatic {
			statics = append(statics, StaticObs{
				ID: id, Key: f.Key, Unit: spec.Unit,
				ValStr: strconv.FormatFloat(val, 'f', -1, 64), TsMs: f.TsMs, Labels: f.Labels,
			})
			continue
		}
```

```go
		series = append(series, SeriesObs{
			ID: id, Key: f.Key, Unit: spec.Unit, Source: spec.Source,
			Counter: spec.Counter, Value: val, TsMs: f.TsMs, Labels: f.Labels,
		})
```

- [ ] **Step 4: Run the full commons gate**

Run: `go build ./... && go test ./...`
Expected: PASS across all packages — Phase 0/1A is additive, so `events`, `langreg`, `langx`, `cmnx`, `kafkax`, `redwin` all stay green.

- [ ] **Step 5: Commit**

```bash
git add langx/extract.go langx/extract_test.go
git commit -m "feat(langx): carry identity labels through Extract

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## What Phase 0 + 1A delivers

After Task 5, `langx.Extract` produces one stable `StaticObs` per JVM memory pool (`jvm_nonheap_limit{pool=Metaspace}`, …), each carrying structured `Labels` — the extraction-side fix for the flip-flood. Commons builds and tests green, and existing consumers are unaffected (everything is additive; `Endpoint` still exists).

## Phase 1B — Consumers (next plan, not in this one)

The end-to-end fix needs the consumers, planned separately because it spans two more repos and the riskiest single edit:
- **metrics-analysis:** `Emitter.Observe(... endpoint string ...)` → `... labels map[string]string ...`; detector key `id|labels.EncodeKey(signal, labels)`; update all four analyzer call sites (web-languages passes `st.Labels`; cmn `nil`; red `{"endpoint": s.Endpoint}`; kafka `{"dim": s.DimLabel}` transitional); thread `st.Labels` into `EventFromFlip`; label-aware `render.go`.
- **arrca-graph:** graph-web-languages `WriteSeries`/`WriteStatic` add a per-entity series index (`g:lang:keys:<id>` SADD) for series; graph-read reader discovers via the index + present static hash fields (replacing `AllSeriesKeys`/`AllStaticKeys`), returning parsed `Labels`; add the index reconciler.
- **commons:** remove `events.Event.Endpoint` once no consumer references it (carry `Labels` only).

## Self-Review

**Spec coverage (Phase 0+1A portion):** §4 primitive (`Labels`, `EncodeKey`/`DecodeKey`, `LabelSpec`) → Task 1; wire contract `Event.Labels` → Task 2; §6 registry `LabelSpec` → Task 3; §7 `langreg` Fold+labels → Tasks 3–4, `langx` carry-through → Task 5. Discovery index, reader switch, emitter endpoint→labels, and `Endpoint` removal are explicitly Phase 1B (separate plan) — not silently dropped.

**Placeholder scan:** none — every code step shows complete code.

**Type consistency:** `Labels`/`map[string]string` used uniformly; `labels.EncodeKey(string, Labels) string` called identically in Task 3 and (next plan) the emitter; `Folded.Labels`/`StaticObs.Labels`/`SeriesObs.Labels`/`Event.Labels` are all `map[string]string`; `identityLabels` returns `(map[string]string, bool)` and is called once in `Fold`.

**Scope:** single repo, additive, independently green — a clean first sub-project; Phase 1B is the next.
