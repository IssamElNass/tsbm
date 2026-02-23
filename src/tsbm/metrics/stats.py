"""
Statistical analysis of benchmark timing samples.

Key design decisions
--------------------
* ``LatencyStats`` is a **Pydantic BaseModel**, not a dataclass, so it
  serialises directly via ``.model_dump()`` for SQLite and Parquet storage
  (Phase 5).
* Percentiles come from an **HDR Histogram** (``hdrh`` module, PyPI package
  ``hdrhistogram``): O(1) recording, constant memory, and numerically correct
  merging.  Range: 1 ns to 60 s with 3 significant digits.
* IQR is computed from the **raw sample list** (not from the histogram) to
  avoid quantisation artefacts at low sample counts.
* Outlier detection uses the **1.5 × IQR fence** (Tukey's method).
"""
from __future__ import annotations

from typing import Sequence

import numpy as np
from hdrh.histogram import HdrHistogram
from pydantic import BaseModel, ConfigDict, Field

from tsbm.metrics.timer import TimingResult

# HDR Histogram bounds (nanoseconds)
_HDR_MIN_NS: int = 1               # 1 ns
_HDR_MAX_NS: int = 60_000_000_000  # 60 s
_HDR_SIG_FIGS: int = 3


# ---------------------------------------------------------------------------
# LatencyStats — Pydantic model for serialisable statistics
# ---------------------------------------------------------------------------


class LatencyStats(BaseModel):
    """
    Summary statistics for a set of latency samples.

    All ``*_ms`` fields are in **milliseconds**.
    ``outlier_count`` is the number of samples beyond the 1.5×IQR fence
    (they are excluded from the HDR histogram percentiles to reduce skew,
    but they are still counted here for transparency).
    """

    model_config = ConfigDict(frozen=True)

    # Percentiles from HDR histogram
    p50_ms:  float = Field(ge=0)
    p95_ms:  float = Field(ge=0)
    p99_ms:  float = Field(ge=0)
    p999_ms: float = Field(ge=0, alias="p99_9_ms")

    # Descriptive stats from raw samples
    mean_ms:   float = Field(ge=0)
    min_ms:    float = Field(ge=0)
    max_ms:    float = Field(ge=0)
    iqr_ms:    float = Field(ge=0)
    stddev_ms: float = Field(ge=0)

    # Outlier info
    outlier_count: int = Field(ge=0)
    sample_count:  int = Field(ge=0)

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def as_dict(self) -> dict[str, float | int]:
        return self.model_dump(by_alias=False)

    def summary_line(self) -> str:
        return (
            f"p50={self.p50_ms:.2f}ms  "
            f"p95={self.p95_ms:.2f}ms  "
            f"p99={self.p99_ms:.2f}ms  "
            f"p99.9={self.p999_ms:.2f}ms  "
            f"mean={self.mean_ms:.2f}ms  "
            f"min={self.min_ms:.2f}ms  "
            f"max={self.max_ms:.2f}ms  "
            f"n={self.sample_count} (outliers={self.outlier_count})"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_stats(latencies_ns: Sequence[int]) -> LatencyStats:
    """
    Compute ``LatencyStats`` from a sequence of nanosecond latency samples.

    Parameters
    ----------
    latencies_ns:
        Raw latency measurements in **nanoseconds** (e.g. from
        ``TimingResult.elapsed_ns``).  Must contain at least one value.

    Returns
    -------
    LatencyStats
        Fully populated statistics object.
    """
    if not latencies_ns:
        return _zero_stats()

    arr = np.asarray(latencies_ns, dtype=np.int64)

    # --- IQR and outlier fence (raw samples) ---
    q1, q3 = float(np.percentile(arr, 25)), float(np.percentile(arr, 75))
    iqr_ns = q3 - q1
    fence_lo = q1 - 1.5 * iqr_ns
    fence_hi = q3 + 1.5 * iqr_ns

    inlier_mask = (arr >= fence_lo) & (arr <= fence_hi)
    outlier_count = int((~inlier_mask).sum())
    inliers = arr[inlier_mask]

    # --- Descriptive stats (raw, all samples) ---
    mean_ms   = float(arr.mean())   / 1e6
    min_ms    = float(arr.min())    / 1e6
    max_ms    = float(arr.max())    / 1e6
    stddev_ms = float(arr.std())    / 1e6
    iqr_ms    = iqr_ns              / 1e6

    # --- HDR Histogram percentiles (inliers only to reduce tail skew) ---
    hist = HdrHistogram(_HDR_MIN_NS, _HDR_MAX_NS, _HDR_SIG_FIGS)
    samples_for_hist = inliers if len(inliers) > 0 else arr
    for ns_val in samples_for_hist:
        # HDR histogram clamps values outside [min, max] so clip here
        clamped = int(max(_HDR_MIN_NS, min(_HDR_MAX_NS, ns_val)))
        hist.record_value(clamped)

    p50_ms  = hist.get_value_at_percentile(50.0)  / 1e6
    p95_ms  = hist.get_value_at_percentile(95.0)  / 1e6
    p99_ms  = hist.get_value_at_percentile(99.0)  / 1e6
    p999_ms = hist.get_value_at_percentile(99.9)  / 1e6

    return LatencyStats(
        p50_ms=p50_ms,
        p95_ms=p95_ms,
        p99_ms=p99_ms,
        p99_9_ms=p999_ms,
        mean_ms=mean_ms,
        min_ms=min_ms,
        max_ms=max_ms,
        iqr_ms=iqr_ms,
        stddev_ms=stddev_ms,
        outlier_count=outlier_count,
        sample_count=len(arr),
    )


def compute_throughput_stats(
    results: Sequence[TimingResult],
) -> dict[str, float]:
    """
    Aggregate throughput metrics across multiple ``TimingResult`` objects.

    Returns a dict with keys:
    ``rows_per_second_mean``, ``rows_per_second_p95``, ``rows_per_second_p99``,
    ``mb_per_second_mean``, ``mb_per_second_p95``, ``mb_per_second_p99``.
    """
    if not results:
        return {
            "rows_per_second_mean": 0.0,
            "rows_per_second_p95":  0.0,
            "rows_per_second_p99":  0.0,
            "mb_per_second_mean":   0.0,
            "mb_per_second_p95":    0.0,
            "mb_per_second_p99":    0.0,
        }

    rps = np.asarray([r.rows_per_second for r in results], dtype=np.float64)
    mbs = np.asarray([r.mb_per_second   for r in results], dtype=np.float64)

    return {
        "rows_per_second_mean": float(rps.mean()),
        "rows_per_second_p95":  float(np.percentile(rps, 95)),
        "rows_per_second_p99":  float(np.percentile(rps, 99)),
        "mb_per_second_mean":   float(mbs.mean()),
        "mb_per_second_p95":    float(np.percentile(mbs, 95)),
        "mb_per_second_p99":    float(np.percentile(mbs, 99)),
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _zero_stats() -> LatencyStats:
    return LatencyStats(
        p50_ms=0.0,
        p95_ms=0.0,
        p99_ms=0.0,
        p99_9_ms=0.0,
        mean_ms=0.0,
        min_ms=0.0,
        max_ms=0.0,
        iqr_ms=0.0,
        stddev_ms=0.0,
        outlier_count=0,
        sample_count=0,
    )
