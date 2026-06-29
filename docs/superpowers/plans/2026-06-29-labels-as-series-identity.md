# Labels as First-Class Series Identity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make data-point labels identity-bearing for runtime metrics so per-pool `jvm.memory.limit` stores one stable series per pool (fixing the limit flip-flood and the chart), via enumerated label values that keep the keyspace statically enumerable.

**Architecture:** Extend `langreg` so a `Signal` may declare enumerated identity `Labels`; `Fold` groups datapoints by `(base key, label set)` and emits a canonical `base{name=value}` key; the key catalogs enumerate `base × values` so graph-read's existing reader is unchanged. `langx` carries the structured labels through; `events.Event` carries them on the wire for rendering. `metrics-analysis` consumes the new commons via a `replace` directive and renders the pool label.

**Tech Stack:** Go 1.25, `go.opentelemetry.io/collector/pdata` (OTLP), miniredis (tests). Repos: `arrca-metrics-commons` (Tasks 1–6), `metrics-analysis` (Task 7).

## Global Constraints

- Spec: `arrca-metrics-commons/docs/superpowers/specs/2026-06-29-labels-as-series-identity-design.md`.
- Empty `Labels` MUST preserve today's behavior byte-for-byte (`encodeKey(base, nil) == base`).
- A single `encodeKey` is the ONE source of truth, used by both `Fold` (writer) and the catalogs (reader enumeration).
- An identity-label value not in the enumerated `Values` causes the datapoint to be dropped.
- No version tag, no deploy — develop via local `replace` directives only.
- TDD: every change starts with a failing test. Commit after each task.
- Commit messages end with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`

## File Structure

- `arrca-metrics-commons/langreg/registry.go` — `LabelSpec`, `Signal.Labels`, `encodeKey`, `Folded.Labels`, `Fold` regroup, `identityLabels`/`contains`, `keysForKind`/`expandLabels`, `jvm.memory.limit` entry.
- `arrca-metrics-commons/langreg/registry_test.go` — fold/encode/catalog tests.
- `arrca-metrics-commons/langx/extract.go` — `SeriesObs.Labels`, `StaticObs.Labels`, carry `Folded.Labels` through.
- `arrca-metrics-commons/langx/extract_test.go` — multi-pool Extract test + helper.
- `arrca-metrics-commons/events/event.go` — `Event.Labels`.
- `arrca-metrics-commons/events/row.go` — encode/decode `labels`.
- `arrca-metrics-commons/events/row_test.go` — row round-trip test.
- `metrics-analysis/go.mod` — `replace` directive.
- `metrics-analysis/internal/anomaly/runtime.go` — `EventFromFlip` carries labels.
- `metrics-analysis/internal/anomaly/render.go` — label-aware runtime-flip rendering.
- `metrics-analysis/cmd/web-languages-metric-analysis/main.go` — pass `st.Labels` through.
- `metrics-analysis/internal/anomaly/render_test.go` — labelled-flip render test.

All commands below assume `cd` into the relevant repo root: `arrca-metrics-commons` for Tasks 1–6, `metrics-analysis` for Task 7.

---

### Task 1: `encodeKey` + `LabelSpec` + `Signal.Labels`

**Files:**
- Modify: `langreg/registry.go`
- Test: `langreg/registry_test.go`

**Interfaces:**
- Produces: `type LabelSpec struct { Name, Attr string; Values []string }`; `Signal.Labels []LabelSpec`; `func encodeKey(base string, labels map[string]string) string`.

- [ ] **Step 1: Write the failing test** — add to `langreg/registry_test.go`:

```go
func TestEncodeKey(t *testing.T) {
	if got := encodeKey("jvm_nonheap_limit", nil); got != "jvm_nonheap_limit" {
		t.Fatalf("empty labels must be base, got %q", got)
	}
	got := encodeKey("jvm_nonheap_limit", map[string]string{"pool": "Metaspace"})
	if got != "jvm_nonheap_limit{pool=Metaspace}" {
		t.Fatalf("single label wrong: %q", got)
	}
	// deterministic ordering regardless of map iteration
	a := encodeKey("k", map[string]string{"y": "2", "x": "1"})
	if a != "k{x=1,y=2}" {
		t.Fatalf("labels must be sorted by name: %q", a)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langreg/ -run TestEncodeKey`
Expected: FAIL — `undefined: encodeKey`.

- [ ] **Step 3: Add the type, field, and helper** — in `langreg/registry.go`, add `"strings"` to the import block, add the type above `Signal`, add the field to `Signal`, and add `encodeKey`:

```go
// LabelSpec declares one identity-bearing datapoint label with an enumerated,
// allow-listed value set. A datapoint whose Attr value is not in Values is
// dropped (it maps to no key).
type LabelSpec struct {
	Name   string   // short label name used in the key + rendering, e.g. "pool"
	Attr   string   // OTLP datapoint attribute, e.g. "jvm.memory.pool.name"
	Values []string // enumerated allowed values
}
```

Add to `Signal` (after `Source`):

```go
	Labels []LabelSpec // identity-bearing labels; empty = legacy single-key behavior
```

Add the helper (near `EffScale`):

```go
// encodeKey builds the canonical key from a base key and an identity label set.
// No labels → base unchanged (legacy keys). Labels are sorted by name for
// determinism: base{n1=v1,n2=v2}. Used by BOTH Fold and the key catalogs.
func encodeKey(base string, labels map[string]string) string {
	if len(labels) == 0 {
		return base
	}
	names := make([]string, 0, len(labels))
	for n := range labels {
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
		b.WriteString(labels[n])
	}
	b.WriteByte('}')
	return b.String()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./langreg/ -run TestEncodeKey`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add langreg/registry.go langreg/registry_test.go
git commit -m "feat(langreg): add LabelSpec, Signal.Labels, encodeKey

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `Fold` groups by `(base, labels)`

**Files:**
- Modify: `langreg/registry.go`
- Test: `langreg/registry_test.go`

**Interfaces:**
- Consumes: `encodeKey`, `Signal.Labels` (Task 1).
- Produces: `Folded.Labels map[string]string`; `Fold` now emits one `Folded` per `(base, label-set)`; `func (s Signal) identityLabels(attrs map[string]string) (map[string]string, bool)`.

- [ ] **Step 1: Write the failing test** — add to `langreg/registry_test.go`:

```go
func TestFoldSplitsByLabel(t *testing.T) {
	s := Signal{
		Kind: KindStatic, FoldAttr: "jvm.memory.type",
		FoldMap: map[string]string{"non_heap": "jvm_nonheap_limit"},
		Labels: []LabelSpec{{Name: "pool", Attr: "jvm.memory.pool.name",
			Values: []string{"Metaspace", "Code Cache"}}},
	}
	got := s.Fold([]Sample{
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "Metaspace"}, Value: 1024, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "Code Cache"}, Value: 117, TsMs: 5},
		{Attrs: map[string]string{"jvm.memory.type": "non_heap", "jvm.memory.pool.name": "G1 Eden Space"}, Value: 9, TsMs: 5}, // unlisted → dropped
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

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langreg/ -run TestFoldSplitsByLabel`
Expected: FAIL — `unknown field 'Labels' in struct literal of type Folded` (and/or wrong count).

- [ ] **Step 3: Add `Folded.Labels`, rewrite `Fold`, add helpers** — in `langreg/registry.go`:

Add to `Folded`:

```go
	Labels map[string]string // identity labels (nil when the signal has none)
```

Replace the whole `Fold` method body with:

```go
func (s Signal) Fold(dps []Sample) []Folded {
	// Fast path: no fold attr and no identity labels → passthrough per datapoint
	// (legacy behavior, no summing).
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
		labels map[string]string
		sum    float64
		tsMs   int64
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
		labels, ok := s.identityLabels(d.Attrs)
		if !ok {
			continue // an identity label value is not enumerated → drop datapoint
		}
		key := encodeKey(base, labels)
		a := m[key]
		if a == nil {
			a = &acc{labels: labels}
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
		out = append(out, Folded{Key: k, Labels: m[k].labels, Value: m[k].sum, TsMs: m[k].tsMs})
	}
	return out
}

// identityLabels extracts the enumerated identity labels from a datapoint's
// attrs. ok=false if any identity label's value is missing or not allow-listed
// (caller must drop the datapoint). Returns nil when s declares no Labels.
func (s Signal) identityLabels(attrs map[string]string) (map[string]string, bool) {
	if len(s.Labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(s.Labels))
	for _, l := range s.Labels {
		v, ok := attrs[l.Attr]
		if !ok || !contains(l.Values, v) {
			return nil, false
		}
		out[l.Name] = v
	}
	return out, true
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./langreg/`
Expected: PASS — including the existing `TestFoldSumsPoolsByType` (fold-no-labels still sums) and `TestFoldNonFoldPassThrough` (fast path).

- [ ] **Step 5: Commit**

```bash
git add langreg/registry.go langreg/registry_test.go
git commit -m "feat(langreg): Fold groups by (base, identity labels)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: catalogs enumerate label combos

**Files:**
- Modify: `langreg/registry.go`
- Test: `langreg/registry_test.go`

**Interfaces:**
- Consumes: `encodeKey`, `Signal.Labels` (Task 1).
- Produces: `func expandLabels(base string, labels []LabelSpec) []string`; `AllSeriesKeys`/`AllStaticKeys` now include enumerated combos.

- [ ] **Step 1: Write the failing test** — add to `langreg/registry_test.go`:

```go
func TestExpandLabels(t *testing.T) {
	got := expandLabels("jvm_nonheap_limit", []LabelSpec{{
		Name: "pool", Attr: "jvm.memory.pool.name", Values: []string{"Metaspace", "Code Cache"},
	}})
	want := map[string]bool{
		"jvm_nonheap_limit{pool=Metaspace}":  true,
		"jvm_nonheap_limit{pool=Code Cache}": true,
	}
	if len(got) != 2 {
		t.Fatalf("want 2 expansions, got %v", got)
	}
	for _, k := range got {
		if !want[k] {
			t.Fatalf("unexpected key %q", k)
		}
	}
	// no labels → just the base
	if base := expandLabels("jvm_threads", nil); len(base) != 1 || base[0] != "jvm_threads" {
		t.Fatalf("no-label expand must be [base], got %v", base)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langreg/ -run TestExpandLabels`
Expected: FAIL — `undefined: expandLabels`.

- [ ] **Step 3: Add `expandLabels` and rewrite `keysForKind`** — in `langreg/registry.go`, replace `keysForKind` and add `expandLabels`:

```go
func keysForKind(kind string) []string {
	seen := map[string]struct{}{}
	for _, s := range registry {
		if s.Kind != kind {
			continue
		}
		var bases []string
		if s.FoldAttr == "" {
			if s.Key != "" {
				bases = append(bases, s.Key)
			}
		} else {
			for _, k := range s.FoldMap {
				bases = append(bases, k)
			}
		}
		for _, base := range bases {
			for _, key := range expandLabels(base, s.Labels) {
				seen[key] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// expandLabels returns every key for base across the cartesian product of the
// signal's enumerated label values. No labels → [base]. (For multi-fold-value
// signals this can yield nominal base×value combos that never receive data;
// those keys simply read empty and are skipped — see spec §4.)
func expandLabels(base string, labels []LabelSpec) []string {
	combos := []map[string]string{{}}
	for _, l := range labels {
		next := make([]map[string]string, 0, len(combos)*len(l.Values))
		for _, c := range combos {
			for _, v := range l.Values {
				nc := make(map[string]string, len(c)+1)
				for k, vv := range c {
					nc[k] = vv
				}
				nc[l.Name] = v
				next = append(next, nc)
			}
		}
		combos = next
	}
	out := make([]string, 0, len(combos))
	for _, c := range combos {
		out = append(out, encodeKey(base, c))
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./langreg/`
Expected: PASS — including existing `TestKeyCatalogs` (still sorted, still disjoint; `jvm_heap_used`/`jvm_heap_limit` still present since those signals have no labels yet).

- [ ] **Step 5: Commit**

```bash
git add langreg/registry.go langreg/registry_test.go
git commit -m "feat(langreg): key catalogs enumerate label combos

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: register per-pool labels on `jvm.memory.limit`

**Files:**
- Modify: `langreg/registry.go`
- Test: `langreg/registry_test.go`

**Interfaces:**
- Consumes: `LabelSpec`, `Fold`, catalogs (Tasks 1–3).

- [ ] **Step 1: Write the failing test** — add to `langreg/registry_test.go`:

```go
func TestMemoryLimitIsPerPool(t *testing.T) {
	s, ok := Lookup("jvm.memory.limit")
	if !ok || len(s.Labels) != 1 || s.Labels[0].Name != "pool" {
		t.Fatalf("jvm.memory.limit must declare a pool label: %+v", s)
	}
	// catalog includes the per-pool static keys
	has := func(xs []string, want string) bool {
		for _, x := range xs {
			if x == want {
				return true
			}
		}
		return false
	}
	if !has(AllStaticKeys(), "jvm_nonheap_limit{pool=Metaspace}") {
		t.Fatalf("static catalog missing per-pool key: %v", AllStaticKeys())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langreg/ -run TestMemoryLimitIsPerPool`
Expected: FAIL — `len(s.Labels) != 1`.

- [ ] **Step 3: Add `Labels` to the registry entry** — in `langreg/registry.go`, replace the `jvm.memory.limit` line with:

```go
	"jvm.memory.limit": {Unit: "MB", Kind: KindStatic, FoldAttr: "jvm.memory.type", FoldMap: map[string]string{"heap": "jvm_heap_limit", "non_heap": "jvm_nonheap_limit"}, Scale: bytesToMB, Source: "java", Labels: []LabelSpec{{
		Name: "pool", Attr: "jvm.memory.pool.name",
		Values: []string{
			"G1 Eden Space", "G1 Survivor Space", "G1 Old Gen",
			"Metaspace", "Compressed Class Space",
			"CodeHeap 'non-nmethods'", "CodeHeap 'profiled nmethods'", "CodeHeap 'non-profiled nmethods'",
		},
	}}},
```

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./langreg/`
Expected: PASS. Note `TestKeyCatalogs`' disjoint check still holds (per-pool static keys don't collide with series keys).

- [ ] **Step 5: Commit**

```bash
git add langreg/registry.go langreg/registry_test.go
git commit -m "feat(langreg): per-pool labels on jvm.memory.limit (Java 21 G1)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: `langx` carries labels through `Extract`

**Files:**
- Modify: `langx/extract.go`
- Test: `langx/extract_test.go`

**Interfaces:**
- Consumes: `Folded.Labels` (Task 2), per-pool `jvm.memory.limit` (Task 4).
- Produces: `SeriesObs.Labels`, `StaticObs.Labels` (both `map[string]string`).

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
	if !ok {
		t.Fatalf("missing Metaspace key: %v", statics)
	}
	if meta.Labels["pool"] != "Metaspace" {
		t.Fatalf("StaticObs.Labels not populated: %+v", meta)
	}
	if _, ok := byKey["jvm_heap_limit{pool=G1 Old Gen}"]; !ok {
		t.Fatalf("missing heap pool key: %v", statics)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./langx/ -run TestExtractPerPoolLimit`
Expected: FAIL — `unknown field 'Labels'` (StaticObs has no Labels yet) / wrong count.

- [ ] **Step 3: Add `Labels` to the obs structs and carry it through** — in `langx/extract.go`:

Change `SeriesObs` and `StaticObs` to add a `Labels` field:

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

In `Extract`, set `Labels: f.Labels` in both append sites (the `KindStatic` branch and the series append):

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

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./langx/`
Expected: PASS — including existing extract tests (their statics/series now carry `Labels: nil`, which is unused there).

- [ ] **Step 5: Commit**

```bash
git add langx/extract.go langx/extract_test.go
git commit -m "feat(langx): carry identity labels through Extract

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: `events.Event.Labels` on the wire + row codec

**Files:**
- Modify: `events/event.go`, `events/row.go`
- Test: `events/row_test.go`

**Interfaces:**
- Produces: `Event.Labels map[string]string` (json `labels,omitempty`); `EncodeRow`/`DecodeRow` round-trip it under the `labels` field.

- [ ] **Step 1: Write the failing test** — add to `events/row_test.go` (create the file if absent, `package events`):

```go
package events

import "testing"

func TestRowLabelsRoundTrip(t *testing.T) {
	e := Event{
		EntityID: "container:default/auth-1/app", Source: "runtime",
		Signal: "jvm_nonheap_limit{pool=Metaspace}", State: StateEvent,
		Unit: "MB", TsMs: 1000, IncidentID: "abc",
		Old: "117", New: "1024",
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

- [ ] **Step 3: Add the field and codec support**

In `events/event.go`, add to the `Event` struct (after `Severity`):

```go
	Labels map[string]string `json:"labels,omitempty"` // identity labels (e.g. pool); rendered by readers
```

In `events/row.go`: add `"encoding/json"` to imports, add the field-name const, encode, and decode.

Add to the field-name `const` block:

```go
	fLabels = "labels"
```

In `EncodeRow`, before `return vals`, append:

```go
	if len(e.Labels) > 0 {
		if b, err := json.Marshal(e.Labels); err == nil {
			vals = append(vals, fLabels, string(b))
		}
	}
```

In `DecodeRow`, before the `return Event{...}`, add:

```go
	var labels map[string]string
	if s := get(fLabels); s != "" {
		_ = json.Unmarshal([]byte(s), &labels)
	}
```

and add `Labels: labels,` to the returned `Event{...}` literal.

- [ ] **Step 4: Run tests to verify pass + no regressions**

Run: `go test ./events/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add events/event.go events/row.go events/row_test.go
git commit -m "feat(events): carry identity Labels on Event + g:anom row

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: `metrics-analysis` consumes new commons + renders the label

**Repo:** `metrics-analysis` (cd into it).

**Files:**
- Modify: `go.mod`
- Modify: `internal/anomaly/runtime.go`
- Modify: `cmd/web-languages-metric-analysis/main.go`
- Modify: `internal/anomaly/render.go`
- Test: `internal/anomaly/render_test.go`

**Interfaces:**
- Consumes: `langx.StaticObs.Labels`, `events.Event.Labels` (Tasks 5–6).
- Produces: `EventFromFlip(entityID, signal, unit, oldVal, newVal string, labels map[string]string, tsMs int64) events.Event` (labels param added before tsMs).

- [ ] **Step 1: Point at local commons via `replace`**

Run:
```bash
go mod edit -replace github.com/arrca-ai/arrca-metrics-commons=../arrca-metrics-commons
go mod tidy
```
Expected: `go.mod` gains a `replace ... => ../arrca-metrics-commons` line; build still resolves.

- [ ] **Step 2: Write the failing render test** — add to `internal/anomaly/render_test.go`:

```go
func TestRenderRuntimeFlipWithPoolLabel(t *testing.T) {
	e := events.Event{
		EntityID: "container:default/auth-1/app", Source: "runtime",
		Signal: "jvm_nonheap_limit{pool=Metaspace}", State: events.StateEvent,
		Unit: "MB", Old: "117", New: "1024",
		Labels: map[string]string{"pool": "Metaspace"},
	}
	s := Render(e)
	if !strings.Contains(s, "JVM max non-heap") || !strings.Contains(s, "Metaspace") {
		t.Fatalf("expected base label + pool in desc, got %q", s)
	}
	if !strings.Contains(s, "117 MB") || !strings.Contains(s, "1024 MB") {
		t.Fatalf("flip values missing: %q", s)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/anomaly/ -run TestRenderRuntimeFlipWithPoolLabel`
Expected: FAIL — desc renders the raw encoded signal (`jvm_nonheap_limit{pool=Metaspace}`), not "JVM max non-heap … Metaspace".

- [ ] **Step 4: Make `render.go` label-aware** — in `internal/anomaly/render.go`, replace `renderRuntimeFlip` and add two helpers (`strings` is already imported):

```go
// renderRuntimeFlip renders a static config-value change (point event), with any
// identity labels (e.g. memory pool) appended.
func renderRuntimeFlip(e events.Event) string {
	old, _ := strconv.ParseFloat(e.Old, 64)
	nw, _ := strconv.ParseFloat(e.New, 64)
	return fmt.Sprintf("%s%s changed %s → %s%s",
		runtimeLabel(baseSignal(e.Signal)), labelSuffix(e.Labels),
		formatValue(old, e.Unit), formatValue(nw, e.Unit), suffixEntity(e.EntityID))
}

// baseSignal strips any encoded label suffix: "jvm_nonheap_limit{pool=X}" → "jvm_nonheap_limit".
func baseSignal(sig string) string {
	if i := strings.IndexByte(sig, '{'); i >= 0 {
		return sig[:i]
	}
	return sig
}

// labelSuffix formats identity labels for a description: " (pool Metaspace)".
func labelSuffix(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	names := make([]string, 0, len(labels))
	for n := range labels {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, n+" "+labels[n])
	}
	return " (" + strings.Join(parts, ", ") + ")"
}
```

Add `"sort"` to the `render.go` import block.

- [ ] **Step 5: Run the render test + existing render tests**

Run: `go test ./internal/anomaly/ -run TestRender`
Expected: PASS — including `TestRenderRuntimeStaticFlipUnchanged` (no labels → `baseSignal` is a no-op, `labelSuffix` empty → identical output).

- [ ] **Step 6: Thread labels from extraction into the flip event** — in `internal/anomaly/runtime.go`, change `EventFromFlip` to accept and set labels:

```go
func EventFromFlip(entityID, signal, unit, oldVal, newVal string, labels map[string]string, tsMs int64) events.Event {
	return events.Event{
		EntityID:   entityID,
		Source:     "runtime",
		Signal:     signal,
		State:      events.StateEvent,
		Unit:       unit,
		TsMs:       tsMs,
		Old:        oldVal,
		New:        newVal,
		Labels:     labels,
		IncidentID: flipEventID(entityID, signal, newVal, tsMs),
	}
}
```

In `cmd/web-languages-metric-analysis/main.go`, update `staticPublisher.ObserveStatic` to take and pass labels, and the call site to pass `st.Labels`:

```go
func (p *staticPublisher) ObserveStatic(entityID, signal, unit, oldVal, newVal string, labels map[string]string, tsMs int64) {
	ev := anomaly.EventFromFlip(entityID, signal, unit, oldVal, newVal, labels, tsMs)
	data, err := ev.Marshal()
	if err != nil {
		return
	}
	p.publish(p.subjectPrefix+"."+itoa(events.Partition(entityID, p.n)), data)
}
```

Call site (in `handle`):

```go
		for _, st := range statics {
			if prev, changed := flips.Observe(st.ID+"|"+st.Key, st.ValStr, time.UnixMilli(st.TsMs)); changed {
				staticPub.ObserveStatic(st.ID, st.Key, st.Unit, prev, st.ValStr, st.Labels, st.TsMs)
			}
		}
```

- [ ] **Step 7: Update the existing `EventFromFlip` callers in tests** — search and fix any test calling the old signature:

Run: `grep -rn "EventFromFlip(" internal/ cmd/`
For each call lacking the labels arg, insert `nil` before the final `tsMs` argument (e.g. `EventFromFlip("id","jvm_heap_limit","MB","512","1024", nil, 1000)`).

- [ ] **Step 8: Full gate**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS, no vet warnings.

- [ ] **Step 9: Commit**

```bash
git add go.mod go.sum internal/anomaly/runtime.go internal/anomaly/render.go internal/anomaly/render_test.go cmd/web-languages-metric-analysis/main.go
git commit -m "feat(anomaly): consume per-pool labels; render pool on runtime flips

Local replace -> ../arrca-metrics-commons (no version tag yet).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:** §4 registry shape → Tasks 1,4. §5 key encoding → Task 1. §6 Fold/key-gen → Tasks 2,3. §7 carry labels (StaticObs/SeriesObs + Event) → Tasks 5,6. §8 storage/read unchanged → satisfied by enumerable catalogs (Task 3; graph-read untouched). §9 anomaly/render → Task 7. §10 non-goals → nothing touches `cmnreg`/`jvm.memory.used`/discovery. §11 testing → each task is TDD. §12 rollout (replace dir, no tag/deploy) → Task 7 Step 1; no deploy step anywhere.

**Placeholder scan:** none — every code step shows full code; the `grep`-and-fix step (Task 7 Step 7) is mechanical with an explicit transformation.

**Type consistency:** `encodeKey(string, map[string]string) string` used identically in Tasks 1/2/3. `Folded.Labels`/`StaticObs.Labels`/`SeriesObs.Labels`/`Event.Labels` are all `map[string]string`. `EventFromFlip` new signature (labels before tsMs) is applied at its definition (Task 7 Step 6) and all call sites (Steps 6–7).

**Scope:** single subsystem (runtime label identity), one plan. graph-read frontend polish is explicitly deferred (spec §8) and not a task.
