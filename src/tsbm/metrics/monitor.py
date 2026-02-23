"""
Background resource monitor.

Runs a daemon thread that samples system-level CPU, memory, and I/O at a
configurable interval.  Results are stored in a bounded in-memory deque and
can be retrieved after the benchmark completes.

Design notes
------------
* Monitors **system-level** resources, not per-process.  The target databases
  run inside Docker containers; psutil cannot see container-internal process
  metrics from the Windows host without elevated privileges or Docker API calls.
* ``psutil.cpu_percent(interval=None)`` is non-blocking (uses the delta since
  the previous call).  The first sample is always 0.0 — this is expected and
  is filtered out in ``get_summary()``.
* Disk and network counters report **deltas** (bytes since last sample), not
  cumulative totals.  This makes the time-series directly plottable.
* ``threading.Lock`` guards reads in ``get_summary()`` and ``stop()`` against
  the writer thread.  Appending to a ``deque`` is GIL-safe, but reading the
  whole deque while the writer is appending is not.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Sequence

import psutil
from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Sample container
# ---------------------------------------------------------------------------


@dataclass
class ResourceSample:
    """A single point-in-time resource snapshot."""

    timestamp_ns:   int    # wall-clock time (time.perf_counter_ns())
    cpu_percent:    float  # system-wide CPU usage [0, 100 × n_cores]
    rss_mb:         float  # resident set size of the Python process (MB)
    vm_used_mb:     float  # total virtual memory used system-wide (MB)
    disk_read_mb:   float  # delta bytes read since last sample (MB)
    disk_write_mb:  float  # delta bytes written since last sample (MB)
    net_sent_mb:    float  # delta bytes sent since last sample (MB)
    net_recv_mb:    float  # delta bytes received since last sample (MB)


# ---------------------------------------------------------------------------
# Summary model (Pydantic, for serialisation in Phase 5)
# ---------------------------------------------------------------------------


class ResourceSummary(BaseModel):
    """Aggregated resource usage statistics over a benchmark run."""

    model_config = ConfigDict(frozen=True)

    cpu_percent_mean:  float = Field(ge=0)
    cpu_percent_max:   float = Field(ge=0)
    rss_mb_mean:       float = Field(ge=0)
    rss_mb_max:        float = Field(ge=0)
    vm_used_mb_mean:   float = Field(ge=0)
    vm_used_mb_max:    float = Field(ge=0)
    disk_read_mb_total:  float = Field(ge=0)
    disk_write_mb_total: float = Field(ge=0)
    net_sent_mb_total:   float = Field(ge=0)
    net_recv_mb_total:   float = Field(ge=0)
    sample_count:      int   = Field(ge=0)
    duration_s:        float = Field(ge=0)

    def as_dict(self) -> dict[str, float | int]:
        return self.model_dump()


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------


class ResourceMonitor:
    """
    Daemon thread that samples system resources at ``interval_ms`` intervals.

    Usage::

        monitor = ResourceMonitor(interval_ms=500)
        monitor.start()
        await run_benchmark(...)
        samples = monitor.stop()
        summary = monitor.get_summary(samples)
    """

    def __init__(
        self,
        interval_ms: int = 500,
        max_samples: int = 10_000,
    ) -> None:
        self._interval_s = interval_ms / 1000.0
        self._samples: deque[ResourceSample] = deque(maxlen=max_samples)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # IO counter baselines (populated on first sample)
        self._prev_disk_io: psutil._common.sdiskio | None = None  # type: ignore[name-defined]
        self._prev_net_io:  psutil._common.snetio  | None = None  # type: ignore[name-defined]

        # Initialise CPU percent baseline (first call always returns 0.0)
        psutil.cpu_percent(interval=None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background sampling thread."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("ResourceMonitor is already running")
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="ResourceMonitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> list[ResourceSample]:
        """
        Signal the monitor to stop and wait for the thread to exit.

        Returns
        -------
        list[ResourceSample]
            All samples collected since ``start()`` was called.
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_s * 3)
        with self._lock:
            samples = list(self._samples)
            self._samples.clear()
        self._thread = None
        return samples

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    @staticmethod
    def get_summary(samples: Sequence[ResourceSample]) -> ResourceSummary:
        """
        Compute a ``ResourceSummary`` from a list of ``ResourceSample`` objects.

        The first sample is excluded from CPU statistics (psutil's first
        ``cpu_percent`` call always returns 0.0).
        """
        if not samples:
            return ResourceSummary(
                cpu_percent_mean=0.0, cpu_percent_max=0.0,
                rss_mb_mean=0.0,      rss_mb_max=0.0,
                vm_used_mb_mean=0.0,  vm_used_mb_max=0.0,
                disk_read_mb_total=0.0, disk_write_mb_total=0.0,
                net_sent_mb_total=0.0,  net_recv_mb_total=0.0,
                sample_count=0, duration_s=0.0,
            )

        import numpy as np

        # Skip the first CPU sample (always 0.0 due to psutil baseline)
        cpu_samples = [s.cpu_percent for s in samples[1:]] or [0.0]
        rss_vals    = [s.rss_mb     for s in samples]
        vm_vals     = [s.vm_used_mb for s in samples]

        duration_s = (
            (samples[-1].timestamp_ns - samples[0].timestamp_ns) / 1e9
            if len(samples) > 1
            else 0.0
        )

        return ResourceSummary(
            cpu_percent_mean=float(np.mean(cpu_samples)),
            cpu_percent_max=float(np.max(cpu_samples)),
            rss_mb_mean=float(np.mean(rss_vals)),
            rss_mb_max=float(np.max(rss_vals)),
            vm_used_mb_mean=float(np.mean(vm_vals)),
            vm_used_mb_max=float(np.max(vm_vals)),
            disk_read_mb_total=sum(s.disk_read_mb  for s in samples),
            disk_write_mb_total=sum(s.disk_write_mb for s in samples),
            net_sent_mb_total=sum(s.net_sent_mb    for s in samples),
            net_recv_mb_total=sum(s.net_recv_mb    for s in samples),
            sample_count=len(samples),
            duration_s=duration_s,
        )

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Main monitoring loop."""
        self_proc = psutil.Process()  # this Python process

        while not self._stop_event.is_set():
            ts = time.perf_counter_ns()

            # CPU (non-blocking — uses delta since last call)
            cpu_pct = psutil.cpu_percent(interval=None)

            # Memory
            rss_mb    = self_proc.memory_info().rss / 1_048_576.0
            vm_info   = psutil.virtual_memory()
            vm_used_mb = (vm_info.total - vm_info.available) / 1_048_576.0

            # Disk I/O deltas
            disk_read_mb, disk_write_mb = self._disk_deltas()

            # Network I/O deltas
            net_sent_mb, net_recv_mb = self._net_deltas()

            sample = ResourceSample(
                timestamp_ns=ts,
                cpu_percent=cpu_pct,
                rss_mb=rss_mb,
                vm_used_mb=vm_used_mb,
                disk_read_mb=disk_read_mb,
                disk_write_mb=disk_write_mb,
                net_sent_mb=net_sent_mb,
                net_recv_mb=net_recv_mb,
            )

            with self._lock:
                self._samples.append(sample)

            self._stop_event.wait(timeout=self._interval_s)

    def _disk_deltas(self) -> tuple[float, float]:
        """Return (read_mb_delta, write_mb_delta) since last call."""
        try:
            current = psutil.disk_io_counters()
        except (AttributeError, RuntimeError):
            return 0.0, 0.0

        if current is None:
            return 0.0, 0.0

        if self._prev_disk_io is None:
            self._prev_disk_io = current
            return 0.0, 0.0

        read_delta  = max(0, current.read_bytes  - self._prev_disk_io.read_bytes)
        write_delta = max(0, current.write_bytes - self._prev_disk_io.write_bytes)
        self._prev_disk_io = current
        return read_delta / 1_048_576.0, write_delta / 1_048_576.0

    def _net_deltas(self) -> tuple[float, float]:
        """Return (sent_mb_delta, recv_mb_delta) since last call."""
        try:
            current = psutil.net_io_counters()
        except (AttributeError, RuntimeError):
            return 0.0, 0.0

        if current is None:
            return 0.0, 0.0

        if self._prev_net_io is None:
            self._prev_net_io = current
            return 0.0, 0.0

        sent_delta = max(0, current.bytes_sent - self._prev_net_io.bytes_sent)
        recv_delta = max(0, current.bytes_recv - self._prev_net_io.bytes_recv)
        self._prev_net_io = current
        return sent_delta / 1_048_576.0, recv_delta / 1_048_576.0
