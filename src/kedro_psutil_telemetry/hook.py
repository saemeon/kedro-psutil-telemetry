from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Protocol

import psutil
from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node

logger = logging.getLogger(__name__)

BYTES2MB = 1 / (1024**2)


# --- Sink protocol & built-in implementations ---


class TelemetrySink(Protocol):
    """Interface for telemetry destinations.

    Called once per metric per sample. Implementations must be thread-safe
    as they are invoked from the background monitoring thread.
    """

    def __call__(
        self, name: str, value: float, step: int, tags: dict[str, Any] | None = None
    ) -> None: ...


def console_sink(
    name: str, value: float, step: int, tags: dict[str, Any] | None = None
) -> None:
    """Sink that logs metrics via the Python logger."""
    tag_str = f" {tags}" if tags else ""
    logger.info("Telemetry %s=%.3f step=%d%s", name, value, step, tag_str)


_mlflow_warned = False


def mlflow_sink(
    name: str, value: float, step: int, tags: dict[str, Any] | None = None
) -> None:
    """Sink that logs metrics to an active MLflow run.

    Tags are not forwarded — MLflow does not support per-metric tags.
    No-op if MLflow is not installed or no run is active.
    """
    global _mlflow_warned
    try:
        import mlflow

        if mlflow.active_run():
            mlflow.log_metric(name, value, step=step)
    except ImportError:
        if not _mlflow_warned:
            logger.warning("mlflow not installed; mlflow_sink is a no-op.")
            _mlflow_warned = True


# --- Hook ---


class PipelinePsutilTelemetry:
    """
    Kedro hook that samples system resources in a background thread and
    dispatches metrics to one or more pluggable sinks.

    Sampled metrics (all can be disabled by passing False):
      - {prefix}.mem.rss_mb   — RSS memory across main process + children
      - {prefix}.mem.swap_mb  — system swap usage
      - {prefix}.cpu.percent  — system-wide CPU utilisation
      - {prefix}.io.read_mb   — disk read delta since last sample
      - {prefix}.io.write_mb  — disk write delta since last sample
      - {prefix}.net.sent_mbs — network bytes sent per second
      - {prefix}.net.recv_mbs — network bytes received per second

    Each metric is tagged with the currently-running Kedro node name.

    Example (settings.py)::

        from kedro_psutil_telemetry.hook import PipelinePsutilTelemetry, mlflow_sink

        HOOKS = (
            PipelinePsutilTelemetry(interval=0.5, sink=mlflow_sink),
        )
    """

    def __init__(
        self,
        *,
        sink: TelemetrySink | list[TelemetrySink] | None = None,
        interval: float = 1.0,
        include_children: bool = True,
        prefix: str = "pipeline",
        memory: str | bool = True,
        swap: str | bool = True,
        cpu: str | bool = True,
        disk_read: str | bool = True,
        disk_write: str | bool = True,
        net_sent: str | bool = True,
        net_recv: str | bool = True,
    ) -> None:
        self.interval = float(interval)
        self.include_children = include_children
        self.prefix = prefix

        # Metric names: True → default name, str → custom name, False → disabled
        self._metric_mem = f"{prefix}.mem.rss_mb" if memory is True else memory
        self._metric_swap = f"{prefix}.mem.swap_mb" if swap is True else swap
        self._metric_cpu = f"{prefix}.cpu.percent" if cpu is True else cpu
        self._metric_disk_read = (
            f"{prefix}.io.read_mb" if disk_read is True else disk_read
        )
        self._metric_disk_write = (
            f"{prefix}.io.write_mb" if disk_write is True else disk_write
        )
        self._metric_net_sent = (
            f"{prefix}.net.sent_mbs" if net_sent is True else net_sent
        )
        self._metric_net_recv = (
            f"{prefix}.net.recv_mbs" if net_recv is True else net_recv
        )

        if sink is None:
            self._sinks: list[TelemetrySink] = [console_sink]
        elif isinstance(sink, list):
            self._sinks = list(sink)
        else:
            self._sinks = [sink]

        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None
        self._proc: psutil.Process | None = None
        self._step: int = 0

        # Written from main thread, read from background thread.
        # String assignment is atomic under CPython's GIL.
        self._current_node: str = "pipeline"

        self._peak_rss_mb: float = 0.0
        self._peak_cpu_pct: float = 0.0

    # --- Kedro hook implementations ---

    @hook_impl(trylast=True)
    def before_pipeline_run(self) -> None:
        """Start the background sampling thread."""
        self._stop_evt.clear()
        self._proc = psutil.Process(os.getpid())
        self._step = 0
        self._current_node = "pipeline"
        self._peak_rss_mb = 0.0
        self._peak_cpu_pct = 0.0

        self._proc.cpu_percent(
            interval=None
        )  # prime CPU baseline (first call returns 0)
        self._net_prev = psutil.net_io_counters()
        self._io_prev = psutil.disk_io_counters()
        self._last_sample_time = time.monotonic()

        self._thread = threading.Thread(
            target=self._sampling_loop, name="system-trace", daemon=True
        )
        self._thread.start()

    @hook_impl
    def before_node_run(self, node: Node) -> None:
        self._current_node = node.name

    @hook_impl
    def after_node_run(self, node: Node) -> None:
        self._current_node = "pipeline"

    @hook_impl(tryfirst=True)
    def after_pipeline_run(self) -> None:
        self._shutdown()

    @hook_impl(trylast=True)
    def on_pipeline_error(self) -> None:
        self._shutdown()

    # --- Internal ---

    def _sampling_loop(self) -> None:
        self._record_sample()  # immediate first sample
        while not self._stop_evt.wait(self.interval):
            self._record_sample()

    def _emit(self, name: str, value: float, tags: dict[str, Any]) -> None:
        for sink in self._sinks:
            try:
                sink(name, value, self._step, tags=tags)
            except Exception as e:
                logger.warning("Sink %r raised an exception: %s", sink, e)

    def _record_sample(self) -> None:
        if not self._proc:
            return

        tags: dict[str, Any] = {"node": self._current_node}

        now = time.monotonic()
        elapsed = now - self._last_sample_time
        self._last_sample_time = now

        try:
            if self._metric_mem:
                total_rss = 0
                for p in self._iter_procs(self._proc):
                    try:
                        total_rss += p.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                rss_mb = total_rss * BYTES2MB
                self._peak_rss_mb = max(self._peak_rss_mb, rss_mb)
                self._emit(self._metric_mem, rss_mb, tags)

            if self._metric_swap:
                swap_mb = psutil.swap_memory().used * BYTES2MB
                self._emit(self._metric_swap, swap_mb, tags)

            if self._metric_cpu:
                cpu_pct = psutil.cpu_percent()
                self._peak_cpu_pct = max(self._peak_cpu_pct, cpu_pct)
                self._emit(self._metric_cpu, cpu_pct, tags)

            if self._metric_disk_read or self._metric_disk_write:
                try:
                    io = psutil.disk_io_counters()
                    if self._metric_disk_read:
                        self._emit(
                            self._metric_disk_read,
                            (io.read_bytes - self._io_prev.read_bytes) * BYTES2MB,
                            tags,
                        )
                    if self._metric_disk_write:
                        self._emit(
                            self._metric_disk_write,
                            (io.write_bytes - self._io_prev.write_bytes) * BYTES2MB,
                            tags,
                        )
                    self._io_prev = io
                except Exception as e:
                    logger.warning("Disk I/O sampling failed: %s", e)

            if self._metric_net_sent or self._metric_net_recv:
                net = psutil.net_io_counters()
                per_sec = (
                    BYTES2MB / elapsed if elapsed > 0 else BYTES2MB / self.interval
                )
                if self._metric_net_sent:
                    self._emit(
                        self._metric_net_sent,
                        (net.bytes_sent - self._net_prev.bytes_sent) * per_sec,
                        tags,
                    )
                if self._metric_net_recv:
                    self._emit(
                        self._metric_net_recv,
                        (net.bytes_recv - self._net_prev.bytes_recv) * per_sec,
                        tags,
                    )
                self._net_prev = net

        except psutil.Error as e:
            logger.debug("System trace sample failed: %s", e)

        self._step += 1

    def _shutdown(self) -> None:
        """Take a final sample, stop the thread, and log peak summary."""
        self._record_sample()
        if self._thread and self._thread.is_alive():
            self._stop_evt.set()
            self._thread.join(timeout=2)
        logger.info(
            "System trace complete — peak RSS: %.1f MB, peak CPU: %.1f%%",
            self._peak_rss_mb,
            self._peak_cpu_pct,
        )

    def _iter_procs(self, root_proc: psutil.Process):
        """Yield root process and optionally its children."""
        if not self.include_children:
            yield root_proc
        else:
            try:
                yield root_proc
                yield from root_proc.children(recursive=True)
            except psutil.Error:
                pass
