# Copyright (c) Simon Niederberger.
# Distributed under the terms of the MIT License.

from .hook import PipelinePsutilTelemetry, TelemetrySink, console_sink, mlflow_sink

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

psutil_telemetry_hook = PipelinePsutilTelemetry()

__all__ = [
    "PipelinePsutilTelemetry",
    "TelemetrySink",
    "console_sink",
    "mlflow_sink",
    "psutil_telemetry_hook",
]
