from __future__ import annotations

import logging
import os
import threading

import mlflow
import psutil
from kedro.framework.hooks import hook_impl

logger = logging.getLogger(__name__)

BYTES2MB = 1 / (1024**2)


class PipelineSystemTrace:
    """
    Kedro hook to trace system resources (Memory, CPU, Disk I/O, Network, Swap)
    during pipeline execution and log them to MLflow.

    Logged metrics:
      - pipeline.mem.rss_mb
      - pipeline.mem.swap_mb
      - pipeline.cpu.percent
      - pipeline.io.read_mb
      - pipeline.io.write_mb
      - pipeline.net.sent_mb
      - pipeline.net.recv_mb
    """

    def __init__(
        self,
        *,
        interval: float = 1.0,
        include_children: bool = True,
        memory: str | bool = "pipeline.mem.rss_mb",
        swap: str | bool = "pipeline.mem.swap_mb",
        cpu: str | bool = "pipeline.cpu.percent",
        disk_read: str | bool = "pipeline.io.read_mb",
        disk_write: str | bool = "pipeline.io.write_mb",
        net_sent: str | bool = "pipeline.net.sent_mbs",
        net_recv: str | bool = "pipeline.net.recv_mbs",
    ) -> None:
        self.interval = float(interval)
        self.include_children = include_children

        self.mem = memory
        self.swap = swap
        self.cpu = cpu
        self.disk_read = disk_read
        self.disk_write = disk_write
        self.net_sent = net_sent
        self.net_recv = net_recv

        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None
        self._proc: psutil.Process | None = None
        self._step: int = 0

    @hook_impl(trylast=True)
    def before_pipeline_run(self) -> None:
        """Start the background system metrics thread."""
        self._stop_evt.clear()
        self._proc = psutil.Process(os.getpid())
        self._step = 0

        self._proc.cpu_percent(interval=None)  # initialise CPU percent baseline
        self._net_prev = psutil.net_io_counters()
        self._io_prev = psutil.disk_io_counters()
        self._thread = threading.Thread(
            target=self._sampling_loop, name="system-trace", daemon=True
        )
        self._thread.start()

    @hook_impl(tryfirst=True)
    def after_pipeline_run(self) -> None:
        self._shutdown()

    @hook_impl(trylast=True)
    def on_pipeline_error(self) -> None:
        self._shutdown()

    def _sampling_loop(self) -> None:
        self._record_sample()  # initial sample
        while not self._stop_evt.wait(self.interval):
            self._record_sample()

    def _record_sample(self) -> None:
        """Collect metrics and log them to MLflow."""
        if not self._proc:
            return

        try:
            processes = self._iter_procs(self._proc)
            if self.mem:
                memory_rss_mb = sum(p.memory_info().rss for p in processes) * BYTES2MB
                mlflow.log_metric(self.mem, memory_rss_mb, step=self._step)

            if self.swap:
                swap_usage_mb = psutil.swap_memory().used / (1024**2)
                mlflow.log_metric(self.swap, swap_usage_mb, step=self._step)

            if self.cpu:
                cpu_usage_percent = psutil.cpu_percent()
                mlflow.log_metric(self.cpu, cpu_usage_percent, step=self._step)

            if self.disk_read or self.disk_write:
                try:
                    io = psutil.disk_io_counters()

                    if self.disk_read:
                        read_mb = (io.read_bytes - self._io_prev.read_bytes) * BYTES2MB
                        mlflow.log_metric(self.disk_read, read_mb, step=self._step)

                    if self.disk_write:
                        write_mb = (
                            io.write_bytes - self._io_prev.write_bytes
                        ) * BYTES2MB
                        mlflow.log_metric(self.disk_write, write_mb, step=self._step)
                    self._io_prev = io
                except Exception as e:
                    logger.warning(e)
            # trace network io since last sample
            if self.net_sent or self.net_recv:
                MB2MBS = BYTES2MB / self.interval
                net = psutil.net_io_counters()

                if self.net_sent:
                    sent_mbs = (net.bytes_sent - self._net_prev.bytes_sent) * MB2MBS
                    mlflow.log_metric(self.net_sent, sent_mbs, step=self._step)

                if self.net_recv:
                    recv_mbs = (net.bytes_recv - self._net_prev.bytes_recv) * MB2MBS
                    mlflow.log_metric(self.net_recv, recv_mbs, step=self._step)

                self._net_prev = net

        except psutil.Error as e:
            logger.debug(f"System trace sample failed: {e}")

        self._step += 1

    def _shutdown(self) -> None:
        """Signal thread to stop and take a final sample."""
        self._record_sample()
        if self._thread and self._thread.is_alive():
            self._stop_evt.set()
            self._thread.join(timeout=2)

    def _iter_procs(self, root_proc: psutil.Process):
        """Yield root process and optionally its children."""
        if not self.include_children:
            yield root_proc
        else:
            try:
                yield root_proc
                yield from root_proc.children(recursive=True)
            except psutil.Error:
                yield root_proc
