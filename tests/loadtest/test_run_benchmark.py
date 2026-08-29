"""Unit tests for loadtest/run_benchmark.py's pure orchestration logic.

No containers, no k6, no real API -- these test the polling/parsing/
aggregation logic in isolation, in-process, fast.
"""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

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
