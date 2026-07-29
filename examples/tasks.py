"""Example task module. Point a worker at it with `taskq worker --tasks examples.tasks`.

For that to work from the repo root, add `src/` to PYTHONPATH or run
`pip install -e .` first, then run from a directory where `examples/` is
importable (or use `PYTHONPATH=.:src`).
"""
from __future__ import annotations

import time

from taskq import register


@register("add")
def add(a: int, b: int) -> int:
    return a + b


@register("slow_add")
def slow_add(a: int, b: int, sleep_s: float = 0.5) -> int:
    time.sleep(sleep_s)
    return a + b


@register("boom")
def boom(message: str = "kaboom") -> None:
    raise RuntimeError(message)


@register("flaky")
def flaky(fail_first: int = 2, _attempts: dict[str, int] | None = None) -> str:
    # Deterministic-ish: each process resets its counter. Good for demos.
    global _FLAKY_COUNTER
    _FLAKY_COUNTER = _FLAKY_COUNTER + 1 if "_FLAKY_COUNTER" in globals() else 1
    if _FLAKY_COUNTER <= fail_first:
        raise RuntimeError(f"flaky failure #{_FLAKY_COUNTER}")
    return "ok"
