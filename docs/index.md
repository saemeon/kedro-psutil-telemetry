# kedro-psutil-telemetry

A Kedro hook that continuously samples system resources in a background thread during pipeline execution and dispatches metrics to pluggable sinks (console, MLflow, or your own).

## Installation

```bash
pip install kedro-psutil-telemetry
```

That's it. Kedro auto-discovers the hook via entry points — no `settings.py` changes needed. Metrics are logged to the console at the default 1-second interval.

With MLflow support:

```bash
pip install kedro-psutil-telemetry[mlflow]
```

## Logging

Kedro's default logging configuration only enables `INFO`-level output for the `kedro` logger namespace. Since `kedro_psutil_telemetry` lives in a separate namespace, its metrics are silently dropped unless you explicitly enable them.

Add a `conf/logging.yml` to your project (Kedro picks this up automatically):

```yaml
version: 1

disable_existing_loggers: False

handlers:
  rich:
    class: kedro.logging.RichHandler
    rich_tracebacks: True

loggers:
  kedro:
    level: INFO
  kedro_psutil_telemetry:
    level: INFO

root:
  handlers: [rich]
```

Without this file you will see the hook registered in `kedro info` but no telemetry output during `kedro run`. This is not a bug in the hook — it is a consequence of how Kedro scopes its default logging.

!!! note
    If you use a custom `conf/logging.yml` already, just add `kedro_psutil_telemetry: {level: INFO}` under the `loggers` key.

## Usage

### Zero-config (auto-discovery)

Install the package and run your pipeline. Kedro registers the hook automatically with default settings (console logging, 1s interval, all metrics enabled).

### Custom configuration

To change the sink, interval, or any metric, register the hook explicitly in `settings.py`:

```python
from kedro_psutil_telemetry import PipelinePsutilTelemetry, mlflow_sink

HOOKS = (
    PipelinePsutilTelemetry(
        sink=mlflow_sink,  # log to MLflow instead of console
        interval=2.0,      # sample every 2 seconds
    ),
)
```

Manual registration takes precedence — Kedro won't double-register the hook.

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

## All options

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
