from __future__ import annotations

import time

import pytest

from bioetl.core.http import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpenError, RetryPolicy


def test_retry_policy_backoff_and_retry_after():
    policy = RetryPolicy(max_retries=3, backoff_factor=0.5, max_backoff_sec=5, jitter=False)

    assert policy.compute_backoff(1) == pytest.approx(0.5)
    assert policy.compute_backoff(2) == pytest.approx(1.0)
    assert policy.compute_backoff(3) == pytest.approx(2.0)

    retry_after = policy.compute_backoff(2, retry_after=3)
    assert retry_after == pytest.approx(3.0)


def test_retry_policy_caps_backoff():
    policy = RetryPolicy(max_retries=5, backoff_factor=2, max_backoff_sec=3, jitter=False)
    assert policy.compute_backoff(5) == 3


def test_circuit_breaker_transitions():
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2, reset_timeout_sec=0.1))
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == "open"
    with pytest.raises(CircuitBreakerOpenError):
        breaker.before_call()
    time.sleep(0.11)
    breaker.before_call()
    breaker.record_success()
    assert breaker.state == "closed"


def test_circuit_breaker_records_failure_on_exception():
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, reset_timeout_sec=1))

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    assert breaker.state == "open"
