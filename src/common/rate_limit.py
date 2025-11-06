from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from threading import Lock, Semaphore
from time import monotonic
from typing import Dict, Iterator


class BudgetExceeded(RuntimeError):
    """Raised when an API budget has been exceeded."""


class CircuitOpen(RuntimeError):
    """Raised when an API circuit breaker is open."""


class RequestBudget:
    """Thread-safe per-key budget tracker."""

    def __init__(self) -> None:
        self._counts: Dict[str, int] = defaultdict(int)
        self._lock = Lock()

    def remaining(self, key: str, limit: int) -> int:
        with self._lock:
            consumed = self._counts[key]
            return max(limit - consumed, 0)

    def consume(self, key: str, limit: int) -> int:
        """Increment usage for ``key`` ensuring the limit is not exceeded."""

        with self._lock:
            consumed = self._counts[key]
            if consumed >= limit:
                raise BudgetExceeded(f"Budget exceeded for key={key}")
            self._counts[key] = consumed + 1
            return limit - self._counts[key]


class KeySemaphore:
    """Semaphore keyed by API identifier to serialize calls."""

    def __init__(self, max_inflight: int) -> None:
        self._max_inflight = max_inflight
        self._semaphores: Dict[str, Semaphore] = {}
        self._lock = Lock()

    @contextmanager
    def acquire(self, key: str) -> Iterator[None]:
        sem = self._get_semaphore(key)
        sem.acquire()
        try:
            yield
        finally:
            sem.release()

    def _get_semaphore(self, key: str) -> Semaphore:
        with self._lock:
            semaphore = self._semaphores.get(key)
            if semaphore is None:
                semaphore = Semaphore(self._max_inflight)
                self._semaphores[key] = semaphore
            return semaphore


class CircuitBreaker:
    """Simple counter-based circuit breaker."""

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: float = 30.0) -> None:
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._lock = Lock()
        self._failures = 0
        self._opened_at: float | None = None

    def allow(self) -> None:
        with self._lock:
            if self._opened_at is None:
                return
            if monotonic() - self._opened_at >= self._cooldown_seconds:
                self._failures = 0
                self._opened_at = None
                return
            raise CircuitOpen("Circuit breaker open; skipping call")

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._failure_threshold:
                self._opened_at = monotonic()
