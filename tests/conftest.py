from __future__ import annotations

import random

import pytest

from taskq.broker import Broker
from taskq.models import RetryPolicy


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self._t = start

    def time(self) -> float:
        return self._t

    def advance(self, s: float) -> None:
        self._t += s


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def broker(clock: FakeClock) -> Broker:
    return Broker(
        default_visibility_s=5.0,
        default_retry=RetryPolicy(max_attempts=3, initial_backoff_s=1.0, jitter_s=0.0),
        clock=clock,
        rng=random.Random(0),
    )
