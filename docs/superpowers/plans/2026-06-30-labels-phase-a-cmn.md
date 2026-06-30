# Labels as Series Identity — Plan Phase A: cmn (cpu/mem/net)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the cmn (cpu/mem/net) pipeline on the unified `(entity, base key, label set)` model: `net` becomes base `net` + a `{direction}` label (replacing the `net_rx`/`net_tx` split), carried as structured labels through extraction, store, read, and anomaly detection.

**Architecture:** Mirror the runtime (langreg/langx/g:lang) migration. `cmnreg` declares an identity `{direction}` label on the network metrics; `cmnx` emits the encoded key `labels.EncodeKey(base, labels)` + a `Labels` map; the graph-metrics store writes under that encoded key (no key-scheme change) plus a generic per-entity series index `g:ts:keys:<id>`; `tsread` discovers via that index and `DecodeKey`s back to base+labels; the cmn analyzer keys anomalies on `(net, {direction})`.

**Tech Stack:** Go 1.25, `arrca-metrics-commons` `labels` pkg, miniredis. Repos: `arrca-metrics-commons` (cmnreg/cmnx), `arrca-graph` (metrics store + tsread reader), `metrics-analysis` (anomaly registry + cmn analyzer).

## Global Constraints

- `labels.EncodeKey(base, l)`: empty labels → `base`; else `base{n=v,...}` (names sorted). `DecodeKey` inverse. One codec everywhere.
- `direction` label values are the OTLP `direction` attr values `receive`/`transmit`; a network datapoint missing `direction` is dropped (mirrors `langreg.identityLabels`).
- cpu/mem stay unlabeled (`Key:"cpu"`/`"mem"`, no labels) → `EncodeKey("cpu", nil) == "cpu"`, byte-identical to today.
- Prototype: clean break, no dual-read compat; read API may be briefly wrong mid-rollout.
- Commons changes need a release; consumers `go get` it after (the controller coordinates the tag — referred to below as **vNEXT**).
- TDD; commit per task; commit messages end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Branch per repo off its main: commons `feat/labels-cmn`, arrca-graph `feat/labels-cmn`, metrics-analysis `feat/labels-cmn`.

## File Structure

- `arrca-metrics-commons/cmnreg/registry.go` — `MetricSpec.Labels`; net entries → base `net` + `{direction}`; `ResolveKey` simplified; new `IdentityLabels`.
- `arrca-metrics-commons/cmnx/extract.go` — `SeriesObs.Labels`; `appendSeries` encodes key + labels.
- `arrca-graph/internal/metrics/keys.go` (new) or inline — `SeriesIndexKey`.
- `arrca-graph/internal/metrics/receiver.go` — SADD the series index on write.
- `arrca-graph/internal/tsread/tsread.go` — discover via index; `MetricSeries.Base`/`.Labels`.
- `metrics-analysis/internal/anomaly/registry.go` — `metrics|net_rx`/`net_tx` → `metrics|net`.
- `metrics-analysis/cmd/cmn-metric-analysis/main.go` — `Observe` base+labels.

---

### Task 1 (commons): `cmnreg` — `{direction}` label on network

**Files:** Modify `cmnreg/registry.go`; Test `cmnreg/registry_test.go`

**Interfaces:**
- Produces: `MetricSpec.Labels []labels.LabelSpec`; `func (s MetricSpec) IdentityLabels(getAttr func(string)(string,bool)) (map[string]string, bool)`; `ResolveKey` now returns `s.Key` (no split). Network entries: `Key:"net"`, `Labels:[]labels.LabelSpec{{Name:"direction", Attr:"direction"}}`.

- [ ] **Step 1: Write the failing test** — add to `cmnreg/registry_test.go` (add import `"github.com/arrca-ai/arrca-metrics-commons/labels"`):

```go
func TestNetIsLabeledByDirection(t *testing.T) {
	s, ok := Lookup("k8s.pod.network.io")
	if !ok || s.Key != "net" || len(s.Labels) != 1 || s.Labels[0].Name != "direction" || s.Labels[0].Attr != "direction" {
		t.Fatalf("net must be base 'net' + {direction} label: %+v", s)
	}
	get := func(m map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	lbls, ok := s.IdentityLabels(get(map[string]string{"direction": "receive"}))
	if !ok || lbls["direction"] != "receive" {
		t.Fatalf("IdentityLabels wrong: %v %v", lbls, ok)
	}
	if _, ok := s.IdentityLabels(get(map[string]string{})); ok {
		t.Fatalf("missing direction attr must drop (ok=false)")
	}
	// cpu has no labels → IdentityLabels returns (nil, true)
	cpu, _ := Lookup("container.cpu.time")
	if l, ok := cpu.IdentityLabels(get(map[string]string{})); !ok || l != nil {
		t.Fatalf("unlabeled metric must yield (nil,true): %v %v", l, ok)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmnreg/ -run TestNetIsLabeledByDirection`
Expected: FAIL — `s.Labels` undefined / `IdentityLabels` undefined / net `Key` is `""`.

- [ ] **Step 3: Implement** — in `cmnreg/registry.go`:

Add import `"github.com/arrca-ai/arrca-metrics-commons/labels"`. Add to `MetricSpec` (after `Source`):
```go
	Labels []labels.LabelSpec // identity-bearing labels (discovery model)
```
Replace the two network registry entries:
```go
	"k8s.pod.network.io":  {Key: "net", Unit: "B/s", Level: ingest.LevelPod, Counter: true, Labels: []labels.LabelSpec{{Name: "direction", Attr: "direction"}}, Source: "daemonset"},
```
```go
	"k8s.node.network.io": {Key: "net", Unit: "B/s", Level: ingest.LevelNode, Counter: true, Labels: []labels.LabelSpec{{Name: "direction", Attr: "direction"}}, Source: "kubeletstats"},
```
Replace `ResolveKey` (drop the split path) and add `IdentityLabels`:
```go
// ResolveKey returns the base stream key for a datapoint. ok=false → drop.
func (s MetricSpec) ResolveKey(getAttr func(string) (string, bool)) (string, bool) {
	return s.Key, s.Key != ""
}

// IdentityLabels extracts the declared identity labels from a datapoint. ok=false
// if any declared label's attr is absent (drop the datapoint). nil when none.
func (s MetricSpec) IdentityLabels(getAttr func(string) (string, bool)) (map[string]string, bool) {
	if len(s.Labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(s.Labels))
	for _, l := range s.Labels {
		v, ok := getAttr(l.Attr)
		if !ok {
			return nil, false
		}
		out[l.Name] = v
	}
	return out, true
}
```
Remove the `SplitAttr`/`SplitMap` fields from `MetricSpec` and any remaining references (no entry uses them after this change). Update the `Key` field comment to `// stream base key: cpu|mem|net`.

- [ ] **Step 4: Run tests** — `go test ./cmnreg/`
Expected: PASS. (Update/replace the old `TestResolveKey_SplitDirection` test — split is gone; assert net now returns base `net` via `ResolveKey` and `{direction}` via `IdentityLabels`.)

- [ ] **Step 5: Commit**
```bash
git add cmnreg/registry.go cmnreg/registry_test.go
git commit -m "feat(cmnreg): net base key + {direction} label (drop split)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2 (commons): `cmnx` — emit encoded key + labels

**Files:** Modify `cmnx/extract.go`; Test `cmnx/extract_test.go`

**Interfaces:**
- Consumes: `MetricSpec.ResolveKey`, `MetricSpec.IdentityLabels` (Task 1), `labels.EncodeKey`.
- Produces: `SeriesObs.Labels map[string]string`; `SeriesObs.Key` is now the **encoded** key (`net{direction=receive}`); the rate-tracker key uses the encoded key.

- [ ] **Step 1: Write the failing test** — add to `cmnx/extract_test.go` a case (or new test) feeding a `k8s.pod.network.io` export with two datapoints (`direction=receive`, `direction=transmit`) and asserting two `SeriesObs` with keys `net{direction=receive}` / `net{direction=transmit}` and `Labels["direction"]` set. (Mirror the existing export-builder helpers in that file.) Example assertion core:

```go
func TestExtractNetPerDirection(t *testing.T) {
	// ... build exist set + a k8s.pod.network.io export with receive+transmit dps,
	// two exports so the counter→rate yields values ...
	_, _, _ = /* call Extract twice as the existing counter test does */ nil, nil, nil
	// second Extract:
	series, _, err := Extract(secondExport, exist, rates, now2)
	if err != nil { t.Fatal(err) }
	byKey := map[string]SeriesObs{}
	for _, s := range series { byKey[s.Key] = s }
	rx, ok := byKey["net{direction=receive}"]
	if !ok || rx.Labels["direction"] != "receive" {
		t.Fatalf("net receive series missing/unlabeled: %+v", series)
	}
	if _, ok := byKey["net{direction=transmit}"]; !ok {
		t.Fatalf("net transmit series missing: %+v", series)
	}
}
```
(Use the file's existing OTLP-build + ExistenceSet + RateTracker helpers verbatim; the implementer fills the build calls from the patterns already in `extract_test.go`.)

- [ ] **Step 2: Run to verify fail** — `go test ./cmnx/ -run TestExtractNetPerDirection`
Expected: FAIL — `SeriesObs.Labels` undefined, and keys are `net_rx`/`net_tx`... actually keys won't exist (net Key is now "net"); the test asserts the encoded forms which aren't produced yet.

- [ ] **Step 3: Implement** — in `cmnx/extract.go`:

Add import `"github.com/arrca-ai/arrca-metrics-commons/labels"`. Add to `SeriesObs`:
```go
	Labels map[string]string
```
Rewrite the `appendSeries` loop body to resolve labels and encode the key:
```go
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		getAttr := ingest.AttrGetter(dp.Attributes())
		base, ok := spec.ResolveKey(getAttr)
		if !ok {
			continue
		}
		lbls, ok := spec.IdentityLabels(getAttr)
		if !ok {
			continue
		}
		key := labels.EncodeKey(base, lbls)
		raw, ok := ingest.NumberValue(dp)
		if !ok {
			continue
		}
		tMs := int64(dp.Timestamp()) / 1_000_000
		val := raw
		if spec.Counter {
			rate, ok := rates.Rate(id+"|"+key, raw, tMs, now)
			if !ok {
				continue
			}
			val = rate
		}
		val *= spec.EffScale()
		out = append(out, SeriesObs{ID: id, Key: key, Unit: spec.Unit, Source: spec.Source, Counter: spec.Counter, Value: val, TsMs: tMs, Labels: lbls})
	}
```

- [ ] **Step 4: Run tests** — `go test ./cmnx/`
Expected: PASS. Existing cpu/mem/limit tests stay green (`EncodeKey("cpu", nil)=="cpu"`; limits untouched). If an existing net test asserts `net_rx`/`net_tx` keys, update it to the encoded forms (legitimate — the key format changed).

- [ ] **Step 5: Commit**
```bash
git add cmnx/extract.go cmnx/extract_test.go
git commit -m "feat(cmnx): emit encoded key + labels for net direction

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

> **RELEASE CHECKPOINT:** after Tasks 1–2 land and commons is green, the controller tags a commons release (**vNEXT**). Tasks 3–6 `go get` it.

---

### Task 3 (arrca-graph): metrics store — per-entity series index

**Files:** Create `internal/metrics/keys.go`; Modify `internal/metrics/receiver.go`; Test `internal/metrics/receiver_test.go`

**Interfaces:**
- Produces: `func SeriesIndexKey(id string) string` → `"g:ts:keys:" + id`; `Receiver.Handle` SADDs each written series' encoded key into it.

- [ ] **Step 1: Write the failing test** — add to `internal/metrics/receiver_test.go` a test that Handles an export producing a `net{direction=receive}` series (the v_NEXT cmnx now emits encoded keys) and asserts `SMEMBERS g:ts:keys:<id>` contains `net{direction=receive}`. (Use the file's existing OTLP-build + miniredis harness.)

- [ ] **Step 2: Run to verify fail** — `go test ./internal/metrics/ -run TestSeriesIndex...`
Expected: FAIL — index key empty / `SeriesIndexKey` undefined.

- [ ] **Step 3: Implement** — create `internal/metrics/keys.go`:
```go
// SPDX-License-Identifier: Apache-2.0
package metrics

// SeriesIndexKey is the per-entity set of (encoded) series keys present for id —
// the discovery index the reader enumerates instead of a fixed key list.
func SeriesIndexKey(id string) string { return "g:ts:keys:" + id }
```
In `receiver.go` `Handle`, inside the `for _, s := range series` loop, after the `XAdd` + `writeMetaOnce`, add:
```go
		pipe.SAdd(ctx, SeriesIndexKey(s.ID), s.Key)
```
(SADD every write is idempotent and cheap; the set self-dedupes. Add an `Expire` on the index next to it mirroring how meta/streams are TTL'd if this store TTLs — check `r.maxLen`/TTL usage; if streams aren't EXPIRE'd here, leave the index without EXPIRE to match.)

- [ ] **Step 4: Run tests** — `go test ./internal/metrics/`
Expected: PASS, existing tests green.

- [ ] **Step 5: Commit**
```bash
git add internal/metrics/keys.go internal/metrics/receiver.go internal/metrics/receiver_test.go
git commit -m "feat(metrics): per-entity series discovery index g:ts:keys

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

(Task 3 also bumps arrca-graph to commons **vNEXT** via `go get` as its first action so the encoded keys flow.)

---

### Task 4 (arrca-graph): tsread reader — discover + decode labels

**Files:** Modify `internal/tsread/tsread.go`; Test `internal/tsread/tsread_test.go`

**Interfaces:**
- Consumes: `metrics.SeriesIndexKey` (Task 3) — but tsread is a different package; replicate the key string `"g:ts:keys:"+id` locally (or import). `labels.DecodeKey` (commons vNEXT).
- Produces: `MetricSeries.Base string` + `.Labels map[string]string`; reader enumerates the index, not the fixed `metricKeys`.

- [ ] **Step 1: Write the failing test** — in `internal/tsread/tsread_test.go`, seed (via miniredis) a `g:ts:<id>:net{direction=receive}` stream + meta + the index `g:ts:keys:<id>` containing `net{direction=receive}` and `cpu`, then assert `Read` returns both, with the net one carrying `Base=="net"` and `Labels["direction"]=="receive"`, and cpu carrying `Base=="cpu"` and nil labels.

- [ ] **Step 2: Run to verify fail** — `go test ./internal/tsread/ -run ...`
Expected: FAIL — `MetricSeries.Base`/`.Labels` undefined; reader still iterates the hardcoded 4 keys (misses encoded net keys).

- [ ] **Step 3: Implement** — in `tsread.go`:

Add import `"github.com/arrca-ai/arrca-metrics-commons/labels"`. Add to `MetricSeries`:
```go
	Base   string            `json:"base,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
```
Delete the `metricKeys` var. In `Read`, replace the `for _, key := range metricKeys` with discovery:
```go
	keys, err := r.rdb.SMembers(ctx, "g:ts:keys:"+id).Result()
	if err != nil {
		return nil, fmt.Errorf("smembers g:ts:keys:%s: %w", id, err)
	}
	out := make([]MetricSeries, 0, len(keys))
	for _, key := range keys {
		// ... unchanged XRange/HGetAll body using `key` ...
		base, lbls := labels.DecodeKey(key)
		s := MetricSeries{Key: key, Base: base, Labels: lbls, Unit: meta["unit"], Type: meta["type"], Series: pts}
		// ... limit parse unchanged ...
		out = append(out, s)
	}
```
(Keep the existing XRange/HGetAll/limit-parse body verbatim, just sourced from the discovered `key`.)

- [ ] **Step 4: Run tests** — `go test ./internal/tsread/`
Expected: PASS. (Update the existing `tsread_test` that relied on the fixed keys to seed the index too — legitimate: discovery now requires the index, which the real writer populates.)

- [ ] **Step 5: Commit**
```bash
git add internal/tsread/tsread.go internal/tsread/tsread_test.go
git commit -m "feat(tsread): discover cmn series via index + decode labels

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5 (metrics-analysis): anomaly registry net → base + analyzer labels

**Files:** Modify `internal/anomaly/registry.go`, `internal/anomaly/registry_test.go`, `cmd/cmn-metric-analysis/main.go`, `go.mod`

**Interfaces:**
- Consumes: commons vNEXT (`labels.DecodeKey`, `cmnx.SeriesObs.Labels`).
- Produces: watch-list entry `metrics|net` (replacing `net_rx`/`net_tx`); analyzer `Observe("metrics", base, id, s.Labels, …)`.

- [ ] **Step 1: bump commons** — `go get github.com/arrca-ai/arrca-metrics-commons@vNEXT && go mod tidy`.

- [ ] **Step 2: Write the failing test** — add to `internal/anomaly/registry_test.go`:
```go
func TestMetricsNetWatched(t *testing.T) {
	if _, ok := LookupSignal("metrics", "net"); !ok {
		t.Fatal("metrics|net must be watched")
	}
	if _, ok := LookupSignal("metrics", "net_rx"); ok {
		t.Fatal("metrics|net_rx must be retired in favor of metrics|net")
	}
}
```

- [ ] **Step 3: Run to verify fail** — `go test ./internal/anomaly/ -run TestMetricsNetWatched`
Expected: FAIL — `metrics|net` absent, `net_rx` present.

- [ ] **Step 4: Implement** — in `internal/anomaly/registry.go`, replace the two net lines with one:
```go
	"metrics|net": {Unit: "B/s", IDPrefix: "pod:", AllowUp: true, AllowDown: true, Floor: 1024, Detector: DetectorAdaptive},
```
In `cmd/cmn-metric-analysis/main.go`, add import `"github.com/arrca-ai/arrca-metrics-commons/labels"` and change the series Observe call:
```go
		for _, s := range series {
			base, _ := labels.DecodeKey(s.Key)
			emitter.Observe("metrics", base, s.ID, s.Labels, s.Value, s.TsMs, 0)
			if base == "cpu" || base == "mem" {
				if v, ok := limitSeen.Load(s.ID + "|" + base); ok {
					if lim, _ := v.(float64); lim > 0 {
						emitter.ObserveLimit(base+"_limit", s.ID, s.Value, lim, s.TsMs)
					}
				}
			}
		}
```
(cpu/mem: `base==s.Key`, labels nil — unchanged behavior. net: base `net`, labels `{direction}` → detector keys per `net{direction=…}`. The limit lookup uses `base` and the `limitSeen` store keys on `l.ID+"|"+l.Key` where `LimitObs.Key` is cpu/mem — unchanged.)

- [ ] **Step 5: Run + full gate** — `go build ./... && go vet ./... && go test ./...`
Expected: PASS. (Any existing test asserting `metrics|net_rx`/`net_tx` is watched → update to `metrics|net`.)

- [ ] **Step 6: Commit**
```bash
git add go.mod go.sum internal/anomaly/registry.go internal/anomaly/registry_test.go cmd/cmn-metric-analysis/main.go
git commit -m "feat(anomaly): cmn net as base+{direction} label; commons vNEXT

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## What Phase A delivers

cmn network series are identified by `(entity, net, {direction})`; the store writes encoded keys + a discovery index; graph-read discovers + decodes labels; the cmn analyzer detects anomalies per `net{direction}`. cpu/mem are byte-identical. The cmn family now matches the runtime model, and a future cmn label (e.g. a per-device split) is a registry one-liner + flows through automatically.

## Self-Review

**Spec coverage:** registry label spec → Task 1; extractor encoded key+labels → Task 2; store discovery index → Task 3; reader discovery+decode → Task 4; anomaly watch-list + analyzer labels → Task 5. Release checkpoint between commons and consumers noted.

**Placeholder scan:** Tasks 2 & 3 test bodies reference "the file's existing OTLP-build helpers" rather than inlining them — the implementer must copy the concrete builders from the adjacent existing tests in those files (named explicitly: `cmnx/extract_test.go`'s counter-rate export builder; `internal/metrics/receiver_test.go`'s export+miniredis harness). All production-code steps are complete.

**Type consistency:** `Labels map[string]string` on `cmnx.SeriesObs`, `MetricSeries`; `IdentityLabels(getAttr) (map[string]string, bool)` defined (Task 1) and called (Task 2); `labels.EncodeKey`/`DecodeKey` match commons signatures; `SeriesIndexKey`/`"g:ts:keys:"+id` consistent between Tasks 3 and 4.

**Scope:** cmn only; bounded `{direction}` dimension; the per-entity index is built generically (reused conceptually by kafka/red phases).
