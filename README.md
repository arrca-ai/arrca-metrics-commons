# arrca-metrics-commons

Shared Go module for arrca's metrics pipeline: extraction primitives (`ingest`),
stateless OTLP extraction (`otlpx`), histogram windowing (`redwin`), the identity-key
codec (`labels`), and the anomaly wire/row contract (`events`). Imported by
`arrca-graph` (writers + read API) and `metrics-analysis` (analyzers + aggregator).
