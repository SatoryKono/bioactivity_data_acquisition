import time

import pytest

from library.utils import rate_limit


@pytest.fixture(autouse=True)
def reset_rate_limits():
    rate_limit.reset_rate_limits()
    yield
    rate_limit.reset_rate_limits()


def test_global_rate_limit_enforced():
    rate_limit.configure_rate_limits(global_limit=rate_limit.RateLimitParams(rps=2, burst=1))
    limiter = rate_limit.get_rate_limiter("client-a")

    start = time.monotonic()
    for _ in range(4):
        limiter.acquire()
    duration = time.monotonic() - start

    # With a 2 RPS limiter and burst of 1, the final three requests should take
    # at least ~1.5 seconds. Allow generous tolerance for runtime jitter.
    assert duration >= 1.4


def test_per_client_limit_overrides_global():
    rate_limit.configure_rate_limits(
        global_limit=rate_limit.RateLimitParams(rps=10, burst=1),
        client_limits={"client-a": rate_limit.RateLimitParams(rps=3, burst=1)},
    )
    limiter = rate_limit.get_rate_limiter("client-a")

    start = time.monotonic()
    for _ in range(5):
        limiter.acquire()
    duration = time.monotonic() - start

    # After the first request, four additional requests at 3 RPS should take at
    # least ~1.3 seconds. Allowing for scheduling noise.
    assert duration >= 1.2
