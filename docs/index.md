# kedro-psutil-telemetry

A Kedro hook that continuously samples system resources in a background thread during pipeline execution and dispatches metrics to pluggable sinks (console, MLflow, or your own).

## Installation

```bash
pip install kedro-psutil-telemetry
```

With MLflow support:

```bash
pip install kedro-psutil-telemetry[mlflow]
```

## Quick Start

Register the hook in your Kedro project's `settings.py`:

```python
from kedro_psutil_telemetry import PipelinePsutilTelemetry

# Log to console (default)
HOOKS = (PipelinePsutilTelemetry(),)
```

```python
from kedro_psutil_telemetry import PipelinePsutilTelemetry, mlflow_sink

# Log to an active MLflow run
HOOKS = (PipelinePsutilTelemetry(sink=mlflow_sink),)
```

## Metrics

All metrics are tagged with the currently-running node name and use the configurable `prefix` (default: `"pipeline"`).

| Metric | Description |
|---|---|
| `{prefix}.mem.rss_mb` | RSS memory of main process + children (MB) |
| `{prefix}.mem.swap_mb` | System swap usage (MB) |
| `{prefix}.cpu.percent` | System-wide CPU utilisation (%) |
| `{prefix}.io.read_mb` | Disk read since last sample (MB) |
| `{prefix}.io.write_mb` | Disk write since last sample (MB) |
| `{prefix}.net.sent_mbs` | Network bytes sent per second (MB/s) |
| `{prefix}.net.recv_mbs` | Network bytes received per second (MB/s) |

A peak-RSS and peak-CPU summary is logged at the end of every run.

## Configuration

```python
HOOKS = (
    PipelinePsutilTelemetry(
        sink=mlflow_sink,      # or a list of sinks, or your own callable
        interval=2.0,          # sample every 2 seconds (default: 1.0)
        prefix="pipeline",     # metric name prefix (default: "pipeline")
        include_children=True, # include child processes (default: True)
        cpu=False,             # disable a metric by passing False
        net_sent=False,
        disk_read="my.reads",  # rename a metric by passing a string
    ),
)
```

## Custom Sinks

Any callable matching `(name: str, value: float, step: int, tags: dict | None) -> None` works as a sink:

```python
def my_sink(name, value, step, tags=None):
    print(f"{name}={value:.2f} @ step {step} node={tags.get('node')}")

HOOKS = (PipelinePsutilTelemetry(sink=my_sink),)
```

## Auto-discovery vs. manual registration

Installing the package registers `PipelinePsutilTelemetry` as a Kedro hook entry point so Kedro can discover it automatically with default settings. For any customisation (`sink`, `interval`, `prefix`, etc.) register the hook manually in `settings.py` as shown above — the entry point instantiates with defaults only.
