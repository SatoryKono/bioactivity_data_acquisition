"""Tests for rate limiter behavior."""

import threading
import time
from collections.abc import Callable

import pytest

from bioetl.core import api_client


def _run_in_thread(target: Callable[[], None], name: str) -> threading.Thread:
    thread = threading.Thread(target=target, name=name)
    thread.daemon = True
    thread.start()
    return thread


def test_token_bucket_does_not_block_parallel_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure threads compute wait time concurrently without holding the lock."""

    limiter = api_client.TokenBucketLimiter(max_calls=1, period=0.2, jitter=False)

    # Deplete the initial token so that subsequent threads must wait.
    limiter.acquire()

    sleep_calls: list[tuple[str, float, float]] = []
    original_sleep = api_client.time.sleep

    def tracking_sleep(duration: float) -> None:
        sleep_calls.append((threading.current_thread().name, time.perf_counter(), duration))
        original_sleep(duration)

    monkeypatch.setattr(api_client.time, "sleep", tracking_sleep)

    def worker() -> None:
        limiter.acquire()

    threads = [
        _run_in_thread(worker, name="worker-1"),
        _run_in_thread(worker, name="worker-2"),
    ]

    for thread in threads:
        thread.join()

    wait_starts: dict[str, float] = {}
    for thread_name, call_time, duration in sleep_calls:
        if duration >= limiter.period * 0.9 and thread_name not in wait_starts:
            wait_starts[thread_name] = call_time

    assert len(wait_starts) == 2, "Both threads should reach the wait state"

    wait_times = sorted(wait_starts.values())
    assert (
        wait_times[1] - wait_times[0] < limiter.period * 0.5
    ), "Second thread waited for the first sleep to finish"

