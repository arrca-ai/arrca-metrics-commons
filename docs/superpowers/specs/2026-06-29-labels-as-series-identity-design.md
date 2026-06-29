# Design: Labels as first-class series identity (unified, discovery-based)

> **Status:** designed (brainstormed + approved), NOT implemented. Supersedes the
> earlier enumerated-values draft of this file (discovery replaces enumeration).
> **Primary repo:** `arrca-metrics-commons`. Consumers: `metrics-analysis`, `arrca-graph`.
> **Date:** 2026-06-29

---

## 1. Problem

A runtime series/static is identified today by **(entity id, key)** with a fixed,
enumerable key. Datapoint attributes (labels) are handled four different,
incompatible ways across the four extractors:

| Extractor | Dimension mechanism | Bounded? |
|---|---|---|
| `langreg` (runtime) | `FoldMap` — collapse pools by type (sum) | — |
| `cmnreg` (infra) | `SplitMap` — split into enumerated keys (`net_rx`/`net_tx`) | bounded |
| `kafkareg` (kafka) | `Dim` enum → `DimLabel` carried in the **`endpoint`** field | **unbounded** (topics) |
| `redwin` (RED) | per-endpoint hash in the **`endpoint`** field | unbounded |

This fragmentation causes real bugs: `jvm.memory.limit` is per-pool, but `FoldMap`
collapses all pools onto one key, so the single key receives a different pool's
value every scrape — a permanent flip cycle that floods the timeline and garbles
the chart. Any new multi-dimensional metric hits one of these idioms' limits.

## 2. Goal

A **single, unified dimension model**: identity = **(entity id, base key, label
map)**, carried structurally end-to-end, with **runtime discovery** of which
series exist (no enumeration). This subsumes all four idioms — including the
unbounded ones (kafka topics) that enumeration cannot express — so the problem
does not recur for any future metric.

## 3. Key decisions (approved)

- **Discovery, not enumeration.** Registries declare identity labels by *attribute*
  only; values are discovered at runtime. Reads use a per-entity index, not a
  static key catalog. (Enumeration cannot represent unbounded kafka topics.)
- **`endpoint` is subsumed into `labels`.** `events.Event.Endpoint` is replaced by
  `Labels map[string]string`. RED endpoint becomes `{endpoint: <hash>}`; kafka
  becomes `{topic, partition}`; JVM becomes `{pool}`. One concept everywhere.
- **Phased rollout.** One shared core, then per-extractor adoption. Each phase is
  its own spec → plan → implement cycle.

## 4. The unified primitive (commons)

- **`Labels`** = `map[string]string` (label name → value), carried on every
  observation, anomaly `Event`, stored row, and read snapshot.
- **`encodeKey(base string, labels map[string]string) string`** — the ONE key
  encoder, used by writers, the detector, and readers. No labels → `base`
  unchanged. With labels → `base{n1=v1,n2=v2}` (names sorted; deterministic).
  Round-trips via `decodeKey` (label values contain no `{}=,`).
- **Registry label spec** — each registry's metric spec declares identity labels:
  `[]LabelSpec{{Name, Attr}}` (no `Values`). Extraction groups datapoints by
  `(base key, label tuple)` and emits one observation per group, carrying the
  structured labels.
- **Discovery index** — per namespace, per entity: `<ns>:keys:<id>` is `SADD`'d
  with each series' encoded key on write; readers `SMEMBERS` it to learn which
  series exist, then read each; a reconciler prunes stale members (mirrors the
  existing `g:anom:ids` reconciler). Helpers live in commons so every store uses
  the same logic.

## 5. Phased rollout

| Phase | Scope | Delivers |
|---|---|---|
| **0 — Commons core** | `Labels` type, `encodeKey`/`decodeKey`, registry `LabelSpec`, wire contract (`Event.Labels`, drop `Endpoint`), read-snapshot labels, discovery-index helpers + reconciler | Foundation; no behavior change on its own |
| **1 — Runtime (`g:lang`)** | `langreg` label spec on `jvm.memory.limit`; `langx` carries labels; graph-web-languages writes encoded keys + index; graph-read discovers; web-languages-metric-analysis renders pool; **emitter `Observe` endpoint→labels (all analyzers)** | **Fixes the JVM pool bug**; proves the model end-to-end |
| **2 — Kafka** | `kafkareg` `Dim`→labels (`topic`,`partition`); kafkax + graph-kafka + kafka analyzer | Kafka on the unified model; retire `Dim`/`DimLabel` |
| **3 — CMN** | `cmnreg` `SplitMap`→labels (`direction`); fix `cmnx` no-sum latent risk; graph-metrics + cmn analyzer | Infra on the unified model |
| **4 — RED** | `redwin` endpoint→`{endpoint}` label; graph-red + red analyzer | RED on the unified model; `endpoint` field fully retired |

**Spec/plan Phase 0 + 1 together first** (foundation + the actual bug fix + an
end-to-end proof). Phases 2–4 repeat the same adoption pattern, one spec each.

## 6. Phase 0 — Commons core (detailed)

- `commons`: new `labels` package (or in `events`): `type Labels = map[string]string`;
  `EncodeKey(base, Labels) string`; `DecodeKey(string) (base string, labels Labels)`.
- `events.Event`: **remove** `Endpoint string`; **add** `Labels map[string]string
  \`json:"labels,omitempty"\``. `EncodeRow`/`DecodeRow`: drop the `endpoint` field;
  add a `labels` field (JSON-encoded map).
- Registry label spec type `LabelSpec{Name, Attr string}` added to each registry's
  spec struct (langreg/cmnreg/kafkareg/redwin) — empty in Phase 0, populated per
  phase.
- Discovery-index helpers in commons: `IndexKey(ns, id) string`,
  and store-agnostic `RecordSeries`/`ListSeries`/`ReconcileIndex` building blocks.
- TDD: encode/decode round-trip incl. endpoint-as-label and multi-label; row
  codec carries labels and no longer carries endpoint.

## 7. Phase 1 — Runtime (detailed)

- `langreg`: `Signal.Labels []LabelSpec`; `Fold` groups by `(base, labels)` and
  emits `Folded{Key, Labels, Value, TsMs}` via `EncodeKey`; `jvm.memory.limit`
  declares `{Name:"pool", Attr:"jvm.memory.pool.name"}`. (No `Values` — discovery.)
- `langx`: `SeriesObs`/`StaticObs` gain `Labels`; `Extract` carries `Folded.Labels`.
- `arrca-graph` graph-web-languages writer: store under the encoded key and `SADD`
  the discovery index (`g:lang:keys:<id>`).
- `arrca-graph` graph-read reader: replace `AllSeriesKeys`/`AllStaticKeys` probing
  with index `SMEMBERS` → read each → return series with parsed `Labels`. Add the
  index reconciler.
- `metrics-analysis` emitter: `Observe(source, signal, entityID string, labels
  map[string]string, value float64, tsMs int64, gateCount uint64)` (was
  `endpoint string`); detector state keyed by `entityID + "|" + EncodeKey(signal,
  labels)`. All four analyzer call sites updated; the runtime flip path carries
  `st.Labels`; cmn/red/kafka call sites pass a one-entry label map wrapping their
  current single dimension until their phase migrates them.
- `metrics-analysis` render: label-aware runtime flip/spike phrasing (uses
  `Event.Labels`).
- TDD: `Fold` per-pool keys+labels; `Extract` multi-pool → one stable static per
  pool; emitter detector keys differ per label set; render shows the pool.

## 7a. Cross-cutting note: the shared emitter

`Emitter.Observe` is called by all four analyzers, so the `endpoint`→`labels`
signature change lands once, in Phase 1, and every call site updates together.
Not-yet-migrated analyzers pass their existing single dimension as a one-entry
label map (e.g. RED `{"endpoint": epHash}`, kafka `{"dim": dimLabel}`) so behavior
is preserved until their phase replaces it with the real structured labels.

## 8. Non-goals / constraints

- No data migration — prototype; old `g:lang`/`g:anom` keys age out or are flushed.
- No version tag, no deploy — develop via local `replace` directives; the owner
  tags + rolls out per phase.
- Frontend label rendering polish is in scope only as far as readers returning
  structured labels; deeper UI work is deferred.

## 9. Testing strategy

Every phase is TDD. Core invariants:
- `EncodeKey`/`DecodeKey` round-trip; empty labels == base (legacy keys unchanged).
- Writer (`Fold`/extract) and reader (discovery) agree on keys.
- Detector state is distinct per label set; identical inputs → identical keys/ids.
- Per-extractor: a multi-dimensional payload yields one stable series per label
  combination (no flip churn / no collapse).

## 10. Blast radius

| Repo | Phase 0 | Phase 1 |
|---|---|---|
| `arrca-metrics-commons` | `labels`/`events` core, registry `LabelSpec`, index helpers | `langreg` Fold+labels, `langx` carry-through |
| `metrics-analysis` | consume core | emitter `Observe` labels, render, all call sites |
| `arrca-graph` | — | graph-web-languages writer + index, graph-read discovery + reconciler |

Later phases extend the same changes to `kafkareg`/`kafkax`/graph-kafka,
`cmnreg`/`cmnx`/graph-metrics, `redwin`/graph-red and their analyzers.
