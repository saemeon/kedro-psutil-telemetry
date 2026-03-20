from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import psutil
import pytest

from kedro_psutil_telemetry import PipelinePsutilTelemetry, console_sink, mlflow_sink
from kedro_psutil_telemetry.hook import BYTES2MB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(name: str = "my_node") -> MagicMock:
    node = MagicMock()
    node.name = name
    return node


def _make_hook(**kwargs) -> PipelinePsutilTelemetry:
    """Return a hook with a no-op sink so nothing is logged during tests."""
    sink = kwargs.pop("sink", MagicMock())
    return PipelinePsutilTelemetry(sink=sink, **kwargs)


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------


def test_default_sink_is_console():
    hook = PipelinePsutilTelemetry()
    assert hook._sinks == [console_sink]


def test_single_sink_wrapped_in_list():
    sink = MagicMock()
    hook = PipelinePsutilTelemetry(sink=sink)
    assert hook._sinks == [sink]


def test_list_of_sinks_stored():
    s1, s2 = MagicMock(), MagicMock()
    hook = PipelinePsutilTelemetry(sink=[s1, s2])
    assert hook._sinks == [s1, s2]


def test_list_of_sinks_is_copied():
    """Mutating the original list must not affect the hook."""
    original = [MagicMock()]
    hook = PipelinePsutilTelemetry(sink=original)
    original.append(MagicMock())
    assert len(hook._sinks) == 1


# ---------------------------------------------------------------------------
# Metric name resolution
# ---------------------------------------------------------------------------


def test_metric_names_default_prefix():
    hook = _make_hook()
    assert hook._metric_mem == "pipeline.mem.rss_mb"
    assert hook._metric_cpu == "pipeline.cpu.percent"


def test_metric_names_custom_prefix():
    hook = _make_hook(prefix="job")
    assert hook._metric_mem == "job.mem.rss_mb"
    assert hook._metric_net_sent == "job.net.sent_mbs"


def test_metric_disabled_by_false():
    hook = _make_hook(memory=False, swap=False)
    assert hook._metric_mem is False
    assert hook._metric_swap is False


def test_metric_custom_name_via_string():
    hook = _make_hook(cpu="custom.cpu")
    assert hook._metric_cpu == "custom.cpu"


# ---------------------------------------------------------------------------
# Node tracking
# ---------------------------------------------------------------------------


def test_before_node_run_sets_current_node():
    hook = _make_hook()
    hook.before_node_run(_make_node("split_data"))
    assert hook._current_node == "split_data"


def test_after_node_run_resets_current_node():
    hook = _make_hook()
    hook.before_node_run(_make_node("split_data"))
    hook.after_node_run(_make_node("split_data"))
    assert hook._current_node == "pipeline"


# ---------------------------------------------------------------------------
# Emit + sink exception isolation
# ---------------------------------------------------------------------------


def test_emit_calls_all_sinks():
    s1, s2 = MagicMock(), MagicMock()
    hook = PipelinePsutilTelemetry(sink=[s1, s2])
    hook._emit("metric", 1.0, {})
    s1.assert_called_once_with("metric", 1.0, 0, tags={})
    s2.assert_called_once_with("metric", 1.0, 0, tags={})


def test_emit_continues_after_sink_exception():
    bad_sink = MagicMock(side_effect=RuntimeError("boom"))
    good_sink = MagicMock()
    hook = PipelinePsutilTelemetry(sink=[bad_sink, good_sink])
    hook._emit("metric", 1.0, {})
    good_sink.assert_called_once()


# ---------------------------------------------------------------------------
# Pipeline lifecycle (using mocked psutil)
# ---------------------------------------------------------------------------


@patch("kedro_psutil_telemetry.hook.psutil.Process")
@patch("kedro_psutil_telemetry.hook.psutil.net_io_counters")
@patch("kedro_psutil_telemetry.hook.psutil.disk_io_counters")
@patch("kedro_psutil_telemetry.hook.psutil.swap_memory")
@patch("kedro_psutil_telemetry.hook.psutil.cpu_percent", return_value=50.0)
def test_full_lifecycle(mock_cpu, mock_swap, mock_disk, mock_net, mock_proc_cls):
    """Hook starts, samples, and stops without raising."""
    # Setup process mock
    proc = MagicMock()
    proc.memory_info.return_value = MagicMock(rss=100 * 1024 * 1024)  # 100 MB
    proc.children.return_value = []
    mock_proc_cls.return_value = proc

    # Setup I/O mocks
    io = MagicMock(read_bytes=0, write_bytes=0)
    mock_disk.return_value = io
    net = MagicMock(bytes_sent=0, bytes_recv=0)
    mock_net.return_value = net
    swap = MagicMock(used=0)
    mock_swap.return_value = swap

    sink = MagicMock()
    hook = PipelinePsutilTelemetry(sink=sink, interval=0.05)
    hook.before_pipeline_run()
    time.sleep(0.15)  # allow ~2-3 samples
    hook.after_pipeline_run()

    assert sink.called
    assert hook._peak_rss_mb == pytest.approx(100.0, abs=1.0)
    assert hook._peak_cpu_pct == pytest.approx(50.0)


@patch("kedro_psutil_telemetry.hook.psutil.Process")
@patch("kedro_psutil_telemetry.hook.psutil.net_io_counters")
@patch("kedro_psutil_telemetry.hook.psutil.disk_io_counters")
@patch("kedro_psutil_telemetry.hook.psutil.swap_memory")
@patch("kedro_psutil_telemetry.hook.psutil.cpu_percent", return_value=0.0)
def test_on_pipeline_error_shuts_down(mock_cpu, mock_swap, mock_disk, mock_net, mock_proc_cls):
    proc = MagicMock()
    proc.memory_info.return_value = MagicMock(rss=0)
    proc.children.return_value = []
    mock_proc_cls.return_value = proc
    mock_disk.return_value = MagicMock(read_bytes=0, write_bytes=0)
    mock_net.return_value = MagicMock(bytes_sent=0, bytes_recv=0)
    mock_swap.return_value = MagicMock(used=0)

    hook = PipelinePsutilTelemetry(sink=MagicMock(), interval=0.05)
    hook.before_pipeline_run()
    hook.on_pipeline_error()

    assert not hook._thread.is_alive()


# ---------------------------------------------------------------------------
# Process iteration safety
# ---------------------------------------------------------------------------


def test_iter_procs_no_children_when_disabled():
    hook = _make_hook(include_children=False)
    proc = MagicMock(spec=psutil.Process)
    result = list(hook._iter_procs(proc))
    assert result == [proc]
    proc.children.assert_not_called()


def test_iter_procs_handles_psutil_error():
    hook = _make_hook(include_children=True)
    proc = MagicMock(spec=psutil.Process)
    proc.children.side_effect = psutil.NoSuchProcess(pid=0)
    result = list(hook._iter_procs(proc))
    assert result == [proc]


def test_iter_procs_skips_dead_child_in_memory_sum():
    """A child dying between children() and memory_info() must not crash the sample."""
    dead_child = MagicMock(spec=psutil.Process)
    dead_child.memory_info.side_effect = psutil.NoSuchProcess(pid=999)

    live_child = MagicMock(spec=psutil.Process)
    live_child.memory_info.return_value = MagicMock(rss=50 * 1024 * 1024)

    proc = MagicMock(spec=psutil.Process)
    proc.memory_info.return_value = MagicMock(rss=100 * 1024 * 1024)
    proc.children.return_value = [dead_child, live_child]

    hook = _make_hook(include_children=True)
    hook._proc = proc
    hook._last_sample_time = time.monotonic()

    with (
        patch("kedro_psutil_telemetry.hook.psutil.swap_memory", return_value=MagicMock(used=0)),
        patch("kedro_psutil_telemetry.hook.psutil.cpu_percent", return_value=0.0),
        patch("kedro_psutil_telemetry.hook.psutil.disk_io_counters") as mock_disk,
        patch("kedro_psutil_telemetry.hook.psutil.net_io_counters") as mock_net,
    ):
        mock_disk.return_value = MagicMock(read_bytes=0, write_bytes=0)
        mock_net.return_value = MagicMock(bytes_sent=0, bytes_recv=0)
        hook._io_prev = MagicMock(read_bytes=0, write_bytes=0)
        hook._net_prev = MagicMock(bytes_sent=0, bytes_recv=0)

        hook._record_sample()  # must not raise

    assert hook._peak_rss_mb == pytest.approx(150.0, abs=1.0)


# ---------------------------------------------------------------------------
# mlflow_sink
# ---------------------------------------------------------------------------


def test_mlflow_sink_noop_when_not_installed():
    """mlflow_sink should not raise when mlflow is absent."""
    import sys
    with patch.dict(sys.modules, {"mlflow": None}):
        mlflow_sink("metric", 1.0, 0)  # must not raise
