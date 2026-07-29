from __future__ import annotations

from taskq.models import RetryPolicy


def test_backoff_monotonic() -> None:
    p = RetryPolicy(initial_backoff_s=1.0, backoff_multiplier=2.0, max_backoff_s=100.0, jitter_s=0.0)
    a = p.backoff_for_attempt(1, 0.0)
    b = p.backoff_for_attempt(2, 0.0)
    c = p.backoff_for_attempt(3, 0.0)
    d = p.backoff_for_attempt(4, 0.0)
    assert (a, b, c, d) == (1.0, 2.0, 4.0, 8.0)


def test_backoff_capped() -> None:
    p = RetryPolicy(initial_backoff_s=1.0, backoff_multiplier=10.0, max_backoff_s=5.0, jitter_s=0.0)
    assert p.backoff_for_attempt(1, 0.0) == 1.0
    assert p.backoff_for_attempt(3, 0.0) == 5.0  # capped
    assert p.backoff_for_attempt(10, 0.0) == 5.0


def test_backoff_jitter_bounded() -> None:
    p = RetryPolicy(initial_backoff_s=1.0, jitter_s=0.5)
    # rand in [0,1) => jitter in [0, 0.5]
    for r in (0.0, 0.3, 0.99):
        v = p.backoff_for_attempt(1, r)
        assert 1.0 <= v <= 1.5
