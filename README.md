# arrca-metrics-commons

Shared Go module for arrca's metrics pipeline: extraction primitives (`ingest`),
per-domain registries (`cmnreg`, `langreg`, `redwin`, `kafkareg`), and the anomaly
wire/row contract (`events`). Imported by `arrca-graph` (writers + read API) and
`graph-metrics-analysis` (analyzers + aggregator).
