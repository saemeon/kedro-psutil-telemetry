[![PyPI](https://img.shields.io/pypi/v/kedro-psutil-telemetry)](https://pypi.org/project/kedro-psutil-telemetry/)
[![Python](https://img.shields.io/pypi/pyversions/kedro-psutil-telemetry)](https://pypi.org/project/kedro-psutil-telemetry/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![ty](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ty/main/assets/badge/v0.json)](https://github.com/astral-sh/ty)
[![prek](https://img.shields.io/badge/prek-checked-blue)](https://github.com/saemeon/prek)

# kedro-psutil-telemetry

A Kedro hook that continuously monitors system resources during pipeline execution and logs them to MLflow.

**Full documentation at [saemeon.github.io/kedro-psutil-telemetry](https://saemeon.github.io/kedro-psutil-telemetry/)**

## Installation

```bash
pip install kedro-psutil-telemetry
```

## Quick Start

Register the hook in your Kedro project's `settings.py`:

```python
from kedro_psutil_telemetry import PipelineSystemTrace

HOOKS = (PipelineSystemTrace(),)
```

Logged MLflow metrics:

| Metric | Description |
|---|---|
| `pipeline.mem.rss_mb` | RSS memory usage (MB) |
| `pipeline.mem.swap_mb` | Swap memory usage (MB) |
| `pipeline.cpu.percent` | CPU utilization (%) |
| `pipeline.io.read_mb` | Disk read since last sample (MB) |
| `pipeline.io.write_mb` | Disk write since last sample (MB) |
| `pipeline.net.sent_mbs` | Network sent since last sample (MB/s) |
| `pipeline.net.recv_mbs` | Network received since last sample (MB/s) |

Customize sampling interval, metric names, or disable individual metrics:

```python
HOOKS = (
    PipelineSystemTrace(
        interval=5.0,          # sample every 5 seconds
        include_children=True, # include child processes
        cpu=False,             # disable CPU tracking
        net_sent=False,        # disable network sent tracking
    ),
)
```

## License

MIT
