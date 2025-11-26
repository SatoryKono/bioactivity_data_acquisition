from __future__ import annotations

import time

from bioetl.core.http import TokenBucketConfig, TokenBucketRateLimiter


def test_token_bucket_timeout_and_try_acquire():
    limiter = TokenBucketRateLimiter(
        TokenBucketConfig(max_tokens=1, refill_period_sec=0.2),
    )
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is False
    # таймаут слишком короткий, токен не успевает наполниться
    assert limiter.acquire(timeout=0.05) is False
    assert limiter.acquire(timeout=0.3) is True


def test_token_bucket_refill_rate():
    limiter = TokenBucketRateLimiter(
        TokenBucketConfig(max_tokens=2, refill_period_sec=0.2),
    )
    assert limiter.acquire(timeout=0) is True
    assert limiter.acquire(timeout=0) is True
    start = time.perf_counter()
    assert limiter.acquire(timeout=0.25) is True
    assert time.perf_counter() - start >= 0.09
