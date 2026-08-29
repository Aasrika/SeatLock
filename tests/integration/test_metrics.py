"""Proves Prometheus multiprocess mode actually aggregates across
processes -- not just that the code compiles.

No Postgres/testcontainers needed, but this genuinely needs separate OS
processes (not just separate objects in one process) to mean anything:
each spawned process re-imports app.infra.metrics fresh and independently
decides in-process-vs-multiprocess mode, exactly like a real uvicorn
worker does. The increment and the read-back run in two DIFFERENT,
freshly-spawned processes (not the same worker reused, which would
trivially "work" even with in-process-only metrics) -- the increment
process fully exits, writing its per-process metric file to disk, before
the read-back process even starts.
"""

from __future__ import annotations

import multiprocessing
import os


def _increment_in_child(multiproc_dir: str) -> None:
    """Runs in its own freshly-spawned process, then exits."""
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = multiproc_dir
    from app.infra.metrics import oversell_blocked_total

    oversell_blocked_total.labels(layer="application").inc()
    oversell_blocked_total.labels(layer="application").inc()


def _read_aggregated_text_in_child(multiproc_dir: str, result_queue: multiprocessing.Queue) -> None:
    """Runs in a SEPARATE freshly-spawned process, started only after the
    incrementing process above has already exited.
    """
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = multiproc_dir
    from app.infra.metrics import render_metrics_text

    result_queue.put(render_metrics_text().decode())


def test_counter_incremented_in_one_process_is_visible_in_aggregated_output(tmp_path):
    multiproc_dir = str(tmp_path / "prometheus-multiproc")
    os.makedirs(multiproc_dir, exist_ok=True)
    ctx = multiprocessing.get_context("spawn")

    incrementer = ctx.Process(target=_increment_in_child, args=(multiproc_dir,))
    incrementer.start()
    incrementer.join(timeout=30)
    assert incrementer.exitcode == 0, "incrementing process did not exit cleanly"

    result_queue: multiprocessing.Queue = ctx.Queue()
    reader = ctx.Process(target=_read_aggregated_text_in_child, args=(multiproc_dir, result_queue))
    reader.start()
    text = result_queue.get(timeout=30)
    reader.join(timeout=30)
    assert reader.exitcode == 0, "reading process did not exit cleanly"

    assert 'oversell_blocked_total{layer="application"} 2.0' in text
