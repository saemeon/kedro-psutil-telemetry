# Changelog

## 0.1.0 (2026-03-20)

### Features

- Pluggable sink architecture: `TelemetrySink` protocol with built-in `console_sink` and `mlflow_sink`
- Node-aware tagging: metrics tagged with the currently-running Kedro node name via `before_node_run` / `after_node_run`
- Configurable metric `prefix` and per-metric enable/disable/rename
- Peak RSS and peak CPU tracked and logged as a summary at pipeline end
- Precise network throughput using actual elapsed time (`time.monotonic()`) rather than nominal interval
- `mlflow` made an optional dependency (`pip install kedro-psutil-telemetry[mlflow]`)
- Kedro hook entry point for auto-discovery with default settings

### Metrics sampled

`{prefix}.mem.rss_mb`, `{prefix}.mem.swap_mb`, `{prefix}.cpu.percent`,
`{prefix}.io.read_mb`, `{prefix}.io.write_mb`, `{prefix}.net.sent_mbs`, `{prefix}.net.recv_mbs`
