# Design: Labels as first-class series identity (runtime/langreg)

> **Status:** designed (brainstormed + approved), NOT implemented.
> **Primary repo:** `arrca-metrics-commons` (`langreg`, `langx`, `events`). Consumers: `metrics-analysis`, `arrca-graph` (graph-web-languages writer, graph-read reader).
> **Date:** 2026-06-29

---

## 1. Problem

A runtime series/static is currently identified by **(entity id, key)**, where `key` is a fixed string the registry produces (`jvm_heap_used`, `jvm_nonheap_limit`). Datapoint attributes (labels) are either folded away (summed by `jvm.memory.type`) or split into enumerated keys (`net_rx`/`net_tx`).

This breaks when a label genuinely dimensions **distinct** series. `jvm.memory.limit` is emitted **per memory pool** (`jvm.memory.pool.name` = Metaspace, Code Cache, Compressed Class Space, G1 Eden/Old/Survivor). The fold keys only on `jvm.memory.type`, collapsing all non-heap pools onto one key `jvm_nonheap_limit`. Because the per-pool datapoints arrive un-summed (separate messages), the single key receives a different pool's value every scrape — observed as a permanent flip cycle `1024 → 117 → 5.5 → 117 → …` (3–4 flips per 30s scrape), flooding the timeline and rendering the chart as garbage.

The same class of bug is latent for any multi-dimensional metric (per-interface network, per-state CPU, per-pool anything).

## 2. Goal

Make **data-point labels a first-class part of series identity**: identity = **(entity id, base key, label set)**. The registry declares which labels are identity-bearing **and enumerates their allowed values**, so the keyspace stays statically enumerable. A label value that isn't enumerated is dropped (with a logged count, not silently). Cardinality is bounded by construction.

**Initial application:** `jvm.memory.limit` (per pool). The capability is general but no other metric is migrated yet (YAGNI).

## 3. Why enumeration (not dynamic discovery)

The registry already hardcodes *which metric names* to listen to — it is the demux. Hardcoding the identity label *values* is consistent with that and has a decisive payoff: `AllSeriesKeys()`/`AllStaticKeys()` can generate `base × label-values`, so graph-read's existing "probe every registered key" reader keeps working **unchanged** — no per-entity index set, no `SCAN`, no reconciler, no reader rewrite. It also bounds cardinality and sidesteps the split-arrival problem (each pool lands in its own key regardless of whether datapoints arrive together).

Trade-off accepted: pool names vary by GC algorithm / JVM version, so the enumerated `Values` list is maintained per metric; an unlisted value is dropped. Acceptable for a prototype where the runtimes are controlled, and a reasonable cardinality guard later.

## 4. Registry change (`langreg`)

```go
// LabelSpec declares one identity-bearing datapoint label with an enumerated,
// allow-listed set of values. A datapoint whose Attr value is not in Values is
// dropped (counted, not silent).
type LabelSpec struct {
    Name   string   // short label name used in the encoded key + rendering, e.g. "pool"
    Attr   string   // OTLP datapoint attribute, e.g. "jvm.memory.pool.name"
    Values []string // enumerated allowed values
}

type Signal struct {
    // ... existing fields (Key, Unit, Kind, Counter, FoldAttr, FoldMap, Scale, Source)
    Labels []LabelSpec // identity-bearing labels; empty = today's behavior (unchanged)
}
```

`jvm.memory.limit` gains (Java 21, default G1 GC — these are `MemoryPoolMXBean.getName()`
values, which the OTel agent reports verbatim as `jvm.memory.pool.name`):
```go
Labels: []LabelSpec{{
    Name: "pool", Attr: "jvm.memory.pool.name",
    Values: []string{
        // heap (G1)
        "G1 Eden Space", "G1 Survivor Space", "G1 Old Gen",
        // non-heap
        "Metaspace", "Compressed Class Space",
        "CodeHeap 'non-nmethods'", "CodeHeap 'profiled nmethods'", "CodeHeap 'non-profiled nmethods'",
    },
}},
```

Notes:
- Pool names are **GC-dependent** (G1 is the Java 21 default; ZGC/Parallel/Serial report different heap-pool names). The enumerated list targets the runtimes we run; unlisted values are dropped.
- Java 21 uses **segmented code cache** (default since Java 9), so there is no `"Code Cache"` — it is the three `CodeHeap '...'` pools above. The two near-identical `~117 MB` non-heap values seen in the live data match two CodeHeap segments.
- Heap and non-heap pools are **disjoint**: the impl groups by `(jvm.memory.type, pool)`, pairing heap pools with `jvm_heap_limit` and non-heap pools with `jvm_nonheap_limit`. Catalog enumeration of any nominal heap×non-heap-pool combo is harmless (no data is ever written there, so the reader probes empty and skips); the implementation may optionally scope `Values` per fold-type to avoid the dead combos.

## 5. Key encoding (single source of truth)

One shared function `encodeKey(base string, labels map[string]string) string` produces the canonical key and is used by **both** `Fold` (writer side) and the `AllSeriesKeys`/`AllStaticKeys` catalogs (reader-enumeration side), guaranteeing writer/reader agreement.

Format: `base` + `{` + sorted `name=value` joined by `,` + `}`, e.g. `jvm_nonheap_limit{pool=Metaspace}`. With `Labels` empty the key is just `base` (today's behavior, byte-for-byte). Label values are used verbatim (Redis keys/stream fields are binary-safe); enumerated pool names contain no `{`, `}`, `=`, `,`, so round-tripping is unambiguous.

## 6. Fold / key generation

`Signal.Fold(dps)` groups datapoints by **(folded base key, tuple of identity-label values)** instead of base key alone, and returns `Folded{Key, Labels, Value, TsMs}` where `Key` is the encoded key and `Labels` is the structured map. Values sharing the same (base, labels) group are summed (handles duplicate datapoints); the latest TsMs wins. When `Labels` is empty the behavior is identical to today (existing `TestFoldSumsPoolsByType` must stay green).

`AllSeriesKeys()`/`AllStaticKeys()` enumerate `base × cartesian(Labels.Values)` via `encodeKey`, sorted.

## 7. Carry labels through the contract

- `langx.StaticObs` / `langx.SeriesObs` gain `Labels map[string]string`.
- `events.Event` gains `Labels map[string]string` (additive; prototype) so anomaly flips/spikes on a label-dimensioned signal can be rendered and queried with their label.

## 8. Storage & read (graph-web-languages / graph-read)

- The g:lang writer stores under the encoded key (static hash field / series stream key) — no logic change, it writes whatever `Key` it's given.
- graph-read's reader is **unchanged for correctness**: the catalogs now enumerate the per-pool combos, so the probe-every-key loop reads them. The chart renders one stable line per pool.
- **Optional polish (deferred):** surface structured labels in the read response (`StaticValue.Labels` / `Series` meta) so the frontend shows `pool=Metaspace` rather than the raw encoded key. Not required for correctness.

## 9. Anomaly / render (metrics-analysis)

- The `jvm_*_limit` flip flood is fixed automatically: each pool is its own stable key, so `FlipTracker` (keyed `id|key`) stops churning — one flip only on a genuine change.
- `render.go` gains label-aware phrasing for runtime flips/spikes (uses `events.Event.Labels`).
- No change to the anomaly detector watch-list for the limit case (limits flow through the static-flip path, not `LookupSignal`).

## 10. Non-goals (YAGNI)

- No per-entity index set or `SCAN` discovery — enumeration makes it unnecessary.
- `cmnreg`'s `SplitAttr`/`SplitMap` (network direction) and `jvm.memory.used` (summed pools) stay as-is. The `Labels` model is designed to subsume them later, but they are not touched now.
- No data migration — prototype; old `g:lang`/`g:anom` keys age out via TTL or are flushed.

## 11. Testing (TDD)

- `encodeKey`: deterministic, sorted, empty-labels == base; round-trip unambiguous.
- `Fold`: per-pool grouping → distinct `base{pool=...}` keys with correct `Labels`; unlisted pool value dropped; non-label signals unchanged (`TestFoldSumsPoolsByType` stays green).
- catalogs: `AllStaticKeys`/`AllSeriesKeys` include the enumerated combos, sorted; writer (`Fold`) and catalog encodings are identical.
- `langx.Extract`: a multi-pool `jvm.memory.limit` payload → one stable static per pool, each carrying `Labels`.
- metrics-analysis: render shows the pool label on a runtime flip; existing flip/onset tests stay green.

## 12. Rollout / mechanics

- Develop across `arrca-metrics-commons`, `metrics-analysis`, and (if touched) `arrca-graph` using local `replace` directives so the unreleased commons is buildable/testable.
- **Do not cut a commons version tag and do not deploy** — the owner does the tag + rollout (graph-web-languages, web-languages-metric-analysis, graph-read) when ready.

## 13. Blast radius summary

| Repo | Change |
|---|---|
| `arrca-metrics-commons` | `langreg` (LabelSpec, Signal.Labels, Fold, encodeKey, catalogs); `langx` (StaticObs/SeriesObs.Labels, Extract carry-through); `events` (Event.Labels) |
| `metrics-analysis` | bump commons (replace dir during dev); `render.go` label-aware phrasing |
| `arrca-graph` | likely zero for correctness; optional reader/frontend polish to surface structured labels |
