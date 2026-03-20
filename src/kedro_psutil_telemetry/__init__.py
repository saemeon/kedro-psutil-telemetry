# Copyright (c) Simon Niederberger.
# Distributed under the terms of the MIT License.

from .hook import PipelinePsutilTelemetry, TelemetrySink, console_sink, mlflow_sink

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

hook = PipelinePsutilTelemetry()

__all__ = [
    "PipelinePsutilTelemetry",
    "TelemetrySink",
    "console_sink",
    "hook",
    "mlflow_sink",
]
