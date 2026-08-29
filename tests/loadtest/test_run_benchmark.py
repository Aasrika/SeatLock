"""Unit tests for loadtest/run_benchmark.py's pure orchestration logic.

No containers, no k6, no real API -- these test the polling/parsing/
aggregation logic in isolation, in-process, fast.
"""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

import pytest

from loadtest import run_benchmark


class TestPollInvariantsUntilDone:
    def test_records_a_violation_that_self_heals_before_the_process_ends(self):
        """The 500ms poller must catch a violation even if it heals before
        the monitored process (k6, in production; a stand-in sleep here)
        exits -- SPEC.md section 10: catching a violation that self-heals
        before the run finishes is the whole point of polling DURING the
        run, not just checking once at the end.
        """
        # Long enough for several 500ms polls; short enough to keep the
        # test fast. First responses show a real violation, later ones show
        # it healed -- by the time poll_invariants_until_done returns, the
        # violation is gone from the *live* state, but must still be in the
        # returned list.
        responses = [
            {
                "all_passed": False,
                "results": {"conservation": {"passed": False, "detail": "boom"}},
            },
            {
                "all_passed": False,
                "results": {"conservation": {"passed": False, "detail": "boom"}},
            },
        ]
        healed = {"all_passed": True, "results": {"conservation": {"passed": True}}}

        def fake_get(url: str, timeout: float = 2.0):
            return responses.pop(0) if responses else healed

        proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", "import time; time.sleep(1.6)"]
        )
        try:
            with patch.object(run_benchmark, "_http_get_json", side_effect=fake_get):
                violations = run_benchmark.poll_invariants_until_done(
                    proc, "http://fake", event_id=1
                )
        finally:
            proc.wait()

        assert len(violations) >= 1
        assert all("timestamp" in v for v in violations)
        assert all(v["results"]["conservation"]["passed"] is False for v in violations)

    def test_records_nothing_when_every_poll_passes(self):
        def fake_get(url: str, timeout: float = 2.0):
            return {"all_passed": True, "results": {}}

        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.6)"])  # noqa: S603
        try:
            with patch.object(run_benchmark, "_http_get_json", side_effect=fake_get):
                violations = run_benchmark.poll_invariants_until_done(
                    proc, "http://fake", event_id=1
                )
        finally:
            proc.wait()

        assert violations == []

    def test_a_transiently_unreachable_api_is_not_itself_recorded_as_a_violation(self):
        """_http_get_json returns None on a transient network failure --
        that's "couldn't check," not "checked and found a violation." The
        poller must not conflate the two.
        """

        def fake_get(url: str, timeout: float = 2.0):
            return None

        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.6)"])  # noqa: S603
        try:
            with patch.object(run_benchmark, "_http_get_json", side_effect=fake_get):
                violations = run_benchmark.poll_invariants_until_done(
                    proc, "http://fake", event_id=1
                )
        finally:
            proc.wait()

        assert violations == []


class TestParseDurationSeconds:
    def test_plain_seconds(self):
        assert run_benchmark._parse_duration_seconds("10s") == 10.0

    def test_minutes_and_seconds(self):
        assert run_benchmark._parse_duration_seconds("1m30s") == 90.0


class TestParseK6Summary:
    def test_missing_metrics_default_to_zero_counts_and_none_trends(self, tmp_path):
        summary = tmp_path / "summary.json"
        summary.write_text('{"metrics": {}}', encoding="utf-8")

        parsed = run_benchmark.parse_k6_summary(summary)

        assert parsed["successes"] == 0
        assert parsed["expected_409s"] == 0
        assert parsed["unexpected_app_errors"] == 0
        assert parsed["transport_failures"] == 0
        assert parsed["p50_ms"] is None
        assert parsed["p95_ms"] is None
        assert parsed["p99_ms"] is None

    def test_extracts_the_confirmed_nested_values_schema(self, tmp_path):
        summary = tmp_path / "summary.json"
        summary.write_text(
            """
            {
              "metrics": {
                "status_2xx": {"values": {"count": 5}},
                "status_409": {"values": {"count": 90}},
                "status_other": {"values": {"count": 2}},
                "status_transport_error": {"values": {"count": 3}},
                "measured_duration_ms": {
                  "values": {"med": 12.5, "p(95)": 60.0, "p(99)": 95.0}
                }
              }
            }
            """,
            encoding="utf-8",
        )

        parsed = run_benchmark.parse_k6_summary(summary)

        assert parsed == {
            "successes": 5,
            "expected_409s": 90,
            "unexpected_app_errors": 2,
            "transport_failures": 3,
            "p50_ms": 12.5,
            "p95_ms": 60.0,
            "p99_ms": 95.0,
        }


class TestParseCounterValue:
    def test_finds_the_matching_line(self):
        text = (
            "# HELP x y\n# TYPE x counter\nsome_other_total 9.0\noptimistic_conflicts_total 5.0\n"
        )
        assert run_benchmark._parse_counter_value(text, "optimistic_conflicts_total") == 5.0

    def test_absent_metric_defaults_to_zero(self):
        assert run_benchmark._parse_counter_value("", "optimistic_conflicts_total") == 0.0

    def test_does_not_match_a_differently_named_metric_with_a_shared_prefix(self):
        # optimistic_conflicts_total vs optimistic_conflicts_total_extra --
        # the trailing space in the match prefix is what prevents this.
        text = "optimistic_conflicts_total_extra 999.0\n"
        assert run_benchmark._parse_counter_value(text, "optimistic_conflicts_total") == 0.0


class TestParseHistogramSumCount:
    def test_finds_both_lines(self):
        text = "optimistic_attempts_sum 12.0\noptimistic_attempts_count 5.0\n"
        total_sum, total_count = run_benchmark._parse_histogram_sum_count(
            text, "optimistic_attempts"
        )
        assert (total_sum, total_count) == (12.0, 5.0)

    def test_absent_metric_defaults_to_zero_zero(self):
        assert run_benchmark._parse_histogram_sum_count("", "optimistic_attempts") == (0.0, 0.0)


def _sweep_run(
    strategy: str,
    ratio: int,
    *,
    rep: int = 1,
    total_request_rps: float | None = None,
    p99_ms: float | None = None,
) -> run_benchmark.SweepRunResult:
    """Minimal SweepRunResult factory for the pure-logic tests below --
    only the fields those functions actually read are meaningfully set.
    """
    return run_benchmark.SweepRunResult(
        strategy=strategy,
        contention_ratio_target=ratio,
        repetition=rep,
        started_at="2026-01-01T00:00:00+00:00",
        seat_count=10,
        vus=200,
        duration_seconds=10.0,
        workers=4,
        pool_size=10,
        max_overflow=5,
        naive_race_window_ms=0,
        total_request_rps=total_request_rps,
        p99_ms=p99_ms,
    )


class TestSweepCellStats:
    def test_mean_min_max_stdev_across_reps(self):
        runs = [
            _sweep_run("optimistic", 5, rep=1, total_request_rps=100.0),
            _sweep_run("optimistic", 5, rep=2, total_request_rps=200.0),
            _sweep_run("optimistic", 5, rep=3, total_request_rps=300.0),
        ]
        stats = run_benchmark._sweep_cell_stats(runs, "total_request_rps")
        assert stats["mean"] == 200.0
        assert stats["min"] == 100.0
        assert stats["max"] == 300.0
        assert stats["stdev"] == pytest.approx(100.0)

    def test_single_rep_has_zero_stdev_not_none(self):
        runs = [_sweep_run("optimistic", 5, total_request_rps=100.0)]
        stats = run_benchmark._sweep_cell_stats(runs, "total_request_rps")
        assert stats["stdev"] == 0.0

    def test_empty_runs_list_is_all_none(self):
        stats = run_benchmark._sweep_cell_stats([], "total_request_rps")
        assert stats == {"mean": None, "min": None, "max": None, "stdev": None}

    def test_none_values_are_excluded_not_treated_as_zero(self):
        runs = [
            _sweep_run("optimistic", 5, total_request_rps=100.0),
            _sweep_run("optimistic", 5, total_request_rps=None),
        ]
        stats = run_benchmark._sweep_cell_stats(runs, "total_request_rps")
        assert stats["mean"] == 100.0  # not 50.0 -- the None must not count as a 0


class TestFindCrossoverInterval:
    def test_finds_the_ratio_pair_where_optimistic_drops_below_pessimistic(self):
        results = {
            ("optimistic", 5): [_sweep_run("optimistic", 5, total_request_rps=500.0)],
            ("pessimistic", 5): [_sweep_run("pessimistic", 5, total_request_rps=300.0)],
            ("optimistic", 20): [_sweep_run("optimistic", 20, total_request_rps=100.0)],
            ("pessimistic", 20): [_sweep_run("pessimistic", 20, total_request_rps=300.0)],
        }
        assert run_benchmark._find_crossover_interval(results, [5, 20]) == (5, 20)

    def test_optimistic_ahead_throughout_returns_none(self):
        results = {
            ("optimistic", 5): [_sweep_run("optimistic", 5, total_request_rps=500.0)],
            ("pessimistic", 5): [_sweep_run("pessimistic", 5, total_request_rps=300.0)],
            ("optimistic", 20): [_sweep_run("optimistic", 20, total_request_rps=400.0)],
            ("pessimistic", 20): [_sweep_run("pessimistic", 20, total_request_rps=300.0)],
        }
        assert run_benchmark._find_crossover_interval(results, [5, 20]) is None

    def test_optimistic_behind_throughout_returns_none(self):
        results = {
            ("optimistic", 5): [_sweep_run("optimistic", 5, total_request_rps=100.0)],
            ("pessimistic", 5): [_sweep_run("pessimistic", 5, total_request_rps=300.0)],
            ("optimistic", 20): [_sweep_run("optimistic", 20, total_request_rps=50.0)],
            ("pessimistic", 20): [_sweep_run("pessimistic", 20, total_request_rps=300.0)],
        }
        assert run_benchmark._find_crossover_interval(results, [5, 20]) is None

    def test_missing_cell_is_skipped_not_crashed_on(self):
        # No pessimistic data at ratio 5 at all -- must not raise.
        results = {
            ("optimistic", 5): [_sweep_run("optimistic", 5, total_request_rps=500.0)],
            ("optimistic", 20): [_sweep_run("optimistic", 20, total_request_rps=100.0)],
            ("pessimistic", 20): [_sweep_run("pessimistic", 20, total_request_rps=300.0)],
        }
        assert run_benchmark._find_crossover_interval(results, [5, 20]) is None


class TestPickRefinementRatios:
    def test_evenly_spaced_strictly_between(self):
        picks = run_benchmark._pick_refinement_ratios(10, 20, count=4)
        assert all(10 < p < 20 for p in picks)
        assert picks == sorted(set(picks))  # deduplicated, sorted

    def test_adjacent_ratios_have_no_room_and_return_empty(self):
        assert run_benchmark._pick_refinement_ratios(10, 11) == []

    def test_narrow_gap_still_only_returns_ratios_strictly_inside(self):
        picks = run_benchmark._pick_refinement_ratios(10, 13, count=4)
        assert all(10 < p < 13 for p in picks)


class TestAssertSweepConfiguration:
    def test_identical_configuration_across_cells_does_not_raise(self):
        results = {
            ("naive", 5): [_sweep_run("naive", 5)],
            ("optimistic", 5): [_sweep_run("optimistic", 5)],
        }
        run_benchmark._assert_sweep_configuration(results)  # must not raise

    def test_mismatched_configuration_raises_system_exit(self):
        mismatched = _sweep_run("optimistic", 5)
        mismatched.vus = 999  # deliberately diverges from the other cell's vus
        results = {
            ("naive", 5): [_sweep_run("naive", 5)],
            ("optimistic", 5): [mismatched],
        }
        with pytest.raises(SystemExit):
            run_benchmark._assert_sweep_configuration(results)
