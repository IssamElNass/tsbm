"""
Unit tests for tsbm.metrics.stats.

Tests:
  - compute_stats on known latency samples
  - HDR histogram percentile accuracy
  - IQR outlier detection (Tukey 1.5×IQR fence)
  - Zero / empty sequence returns all-zero LatencyStats
  - Single-element sequence
  - compute_throughput_stats aggregation
"""
from __future__ import annotations

import math

import pytest

from tsbm.metrics.stats import LatencyStats, compute_stats, compute_throughput_stats
from tsbm.metrics.timer import TimingResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MS = 1_000_000  # nanoseconds per millisecond


def ns(*millis: float) -> list[int]:
    """Convert millisecond values to nanoseconds."""
    return [int(m * _MS) for m in millis]


# ---------------------------------------------------------------------------
# compute_stats — basic correctness
# ---------------------------------------------------------------------------


class TestComputeStats:
    def test_returns_latency_stats_instance(self) -> None:
        result = compute_stats(ns(1, 2, 3, 4, 5))
        assert isinstance(result, LatencyStats)

    def test_empty_sequence_returns_zero_stats(self) -> None:
        result = compute_stats([])
        assert result.p50_ms == 0.0
        assert result.p99_ms == 0.0
        assert result.mean_ms == 0.0
        assert result.sample_count == 0
        assert result.outlier_count == 0

    def test_single_element(self) -> None:
        result = compute_stats(ns(5.0))
        assert result.sample_count == 1
        assert result.min_ms == pytest.approx(5.0, rel=0.01)
        assert result.max_ms == pytest.approx(5.0, rel=0.01)
        assert result.mean_ms == pytest.approx(5.0, rel=0.01)

    def test_uniform_values(self) -> None:
        # All the same → p50 == p99 == mean
        result = compute_stats(ns(*([10.0] * 100)))
        assert result.p50_ms == pytest.approx(10.0, rel=0.01)
        assert result.p99_ms == pytest.approx(10.0, rel=0.01)
        assert result.mean_ms == pytest.approx(10.0, rel=0.01)
        assert result.iqr_ms == pytest.approx(0.0, abs=0.1)

    def test_sample_count_matches_input(self) -> None:
        samples = ns(1, 2, 3, 4, 5, 10, 20, 50)
        result = compute_stats(samples)
        assert result.sample_count == len(samples)

    def test_min_max_correct(self) -> None:
        result = compute_stats(ns(3, 1, 7, 2, 9))
        assert result.min_ms == pytest.approx(1.0, rel=0.01)
        assert result.max_ms == pytest.approx(9.0, rel=0.01)

    def test_mean_correct(self) -> None:
        # mean of [1,2,3,4,5] ms = 3 ms
        result = compute_stats(ns(1, 2, 3, 4, 5))
        assert result.mean_ms == pytest.approx(3.0, rel=0.05)

    def test_percentiles_monotonically_increasing(self) -> None:
        result = compute_stats(ns(*range(1, 101)))
        assert result.p50_ms <= result.p95_ms <= result.p99_ms <= result.p999_ms

    def test_stddev_positive_for_varied_data(self) -> None:
        result = compute_stats(ns(1, 5, 10, 50, 100))
        assert result.stddev_ms > 0

    def test_p50_near_median(self) -> None:
        # 100 evenly-spaced samples [1 ms … 100 ms] → median ≈ 50 ms
        result = compute_stats(ns(*range(1, 101)))
        assert result.p50_ms == pytest.approx(50.0, abs=2.0)

    def test_p99_near_99th_percentile(self) -> None:
        result = compute_stats(ns(*range(1, 101)))
        # p99 of 1..100 ms ≈ 99 ms (HDR quantises, so allow ±2ms)
        assert result.p99_ms == pytest.approx(99.0, abs=2.0)

    def test_all_fields_non_negative(self) -> None:
        result = compute_stats(ns(1, 2, 3))
        for field_name, value in result.model_dump(by_alias=False).items():
            assert value >= 0, f"Field {field_name!r} is negative: {value}"


# ---------------------------------------------------------------------------
# IQR outlier detection
# ---------------------------------------------------------------------------


class TestOutlierDetection:
    def test_no_outliers_in_uniform_data(self) -> None:
        result = compute_stats(ns(*([5.0] * 50)))
        assert result.outlier_count == 0

    def test_clear_high_outlier_detected(self) -> None:
        # 99 inliers at 1 ms + 1 extreme outlier at 10 000 ms
        samples = ns(*([1.0] * 99), 10_000.0)
        result = compute_stats(samples)
        assert result.outlier_count >= 1

    def test_clear_low_outlier_detected(self) -> None:
        # 99 inliers at 100 ms + 1 extreme low at 0.001 ms
        samples = ns(*([100.0] * 99), 0.001)
        result = compute_stats(samples)
        assert result.outlier_count >= 1

    def test_outlier_count_does_not_exceed_sample_count(self) -> None:
        result = compute_stats(ns(1, 2, 3, 1000, 2000))
        assert result.outlier_count <= result.sample_count

    def test_iqr_positive_for_spread_data(self) -> None:
        result = compute_stats(ns(*range(1, 101)))
        assert result.iqr_ms > 0

    def test_percentiles_exclude_extreme_outliers(self) -> None:
        # If outlier removal works, p99 of 1..100ms should not be dominated by outlier
        samples = ns(*range(1, 101), 1_000_000.0)  # last = 1 000 s
        result = compute_stats(samples)
        # p99 should still be in the ~100 ms range, not millions
        assert result.p99_ms < 10_000.0


# ---------------------------------------------------------------------------
# LatencyStats model
# ---------------------------------------------------------------------------


class TestLatencyStatsModel:
    def test_is_frozen(self) -> None:
        result = compute_stats(ns(1, 2, 3))
        with pytest.raises(Exception):
            result.p50_ms = 999.0  # type: ignore[misc]

    def test_as_dict_returns_plain_dict(self) -> None:
        result = compute_stats(ns(1, 2, 3))
        d = result.as_dict()
        assert isinstance(d, dict)
        assert "p50_ms" in d
        assert "p99_ms" in d
        assert "mean_ms" in d

    def test_summary_line_is_string(self) -> None:
        result = compute_stats(ns(1, 2, 3))
        line = result.summary_line()
        assert isinstance(line, str)
        assert "p50=" in line
        assert "p99=" in line


# ---------------------------------------------------------------------------
# compute_throughput_stats
# ---------------------------------------------------------------------------


class TestThroughputStats:
    def test_empty_returns_zeros(self) -> None:
        result = compute_throughput_stats([])
        assert result["rows_per_second_mean"] == 0.0
        assert result["mb_per_second_mean"] == 0.0

    def test_single_result(self) -> None:
        t = TimingResult(
            elapsed_ns=1_000_000_000,  # 1 second
            rows_processed=10_000,
            bytes_processed=10_000 * 100,
        )
        result = compute_throughput_stats([t])
        assert result["rows_per_second_mean"] == pytest.approx(10_000.0, rel=0.01)

    def test_mean_across_multiple(self) -> None:
        timings = [
            TimingResult(elapsed_ns=1_000_000_000, rows_processed=10_000, bytes_processed=0),
            TimingResult(elapsed_ns=1_000_000_000, rows_processed=20_000, bytes_processed=0),
        ]
        result = compute_throughput_stats(timings)
        assert result["rows_per_second_mean"] == pytest.approx(15_000.0, rel=0.01)

    def test_all_keys_present(self) -> None:
        result = compute_throughput_stats([
            TimingResult(elapsed_ns=1_000_000_000, rows_processed=1, bytes_processed=0)
        ])
        expected_keys = {
            "rows_per_second_mean", "rows_per_second_p95", "rows_per_second_p99",
            "mb_per_second_mean", "mb_per_second_p95", "mb_per_second_p99",
        }
        assert expected_keys <= result.keys()
