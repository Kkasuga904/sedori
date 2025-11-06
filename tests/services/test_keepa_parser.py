from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import requests

from src.common.config import CacheSettings, KeepaSettings, RetrySettings
from src.common.rate_limit import CircuitBreaker, KeySemaphore, RequestBudget
from src.services.keepa_api import KEEPA_EPOCH, KeepaAPIClient


class RecordingSession:
    def __init__(self, responses: list[requests.Response]):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url: str, *, params: dict, timeout: tuple):
        idx = min(self.calls, len(self._responses) - 1)
        self.calls += 1
        return self._responses[idx]


def _make_response(payload: dict) -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(payload).encode("utf-8")
    return response


def _compact_series(points: list[tuple[datetime, int]]) -> list[int]:
    entries: list[int] = []
    previous_minutes = None
    for timestamp, value in sorted(points, key=lambda item: item[0]):
        minutes = int((timestamp - KEEPA_EPOCH).total_seconds() // 60)
        if previous_minutes is None:
            entries.extend([minutes, value])
        else:
            entries.extend([minutes - previous_minutes, value])
        previous_minutes = minutes
    return entries


def _build_client(session: RecordingSession) -> KeepaAPIClient:
    settings = KeepaSettings(api_key="dummy", domain=5)
    retry = RetrySettings(max_attempts=2, base=0.01, max_sleep=0.01)
    cache = CacheSettings(ttl_seconds=1, cleanup_interval=1)
    budget = RequestBudget()
    semaphore = KeySemaphore(max_inflight=1)
    circuit = CircuitBreaker()
    return KeepaAPIClient(
        settings,
        retry,
        cache,
        budget=budget,
        budget_limit=5,
        semaphore=semaphore,
        circuit_breaker=circuit,
        session=session,
    )


def test_keepa_price_summary_30d() -> None:
    now = datetime.now(timezone.utc)
    price_points = [
        (now - timedelta(days=5), 150000),
        (now - timedelta(days=4), 160000),
        (now - timedelta(days=3), 140000),
        (now - timedelta(days=2), 155000),
    ]
    rank_points = [
        (now - timedelta(days=5), 5000),
        (now - timedelta(days=2), 4800),
    ]
    payload = {
        "currency": "JPY",
        "products": [
            {
                "csv": {
                    "AMAZON": _compact_series(price_points),
                    "SALES": _compact_series(rank_points),
                },
                "title": "Test Product",
                "imagesCSV": "ABC123",
            }
        ],
    }
    session = RecordingSession([_make_response(payload)])
    client = _build_client(session)

    result = client.get_price_snapshot(SimpleNamespace(asin="ASIN", barcode=None))
    assert result.data is not None
    snapshot = result.data
    assert snapshot.current_price == Decimal("1550.00")
    assert snapshot.average_price_30d == Decimal("1525.00")
    assert snapshot.lowest_price_30d == Decimal("1430.00")
    assert snapshot.highest_price_30d == Decimal("1585.00")
    assert snapshot.sales_rank == 4800
    assert result.flags.degraded is False


def test_keepa_sparse_data_degrades() -> None:
    old = datetime.now(timezone.utc) - timedelta(days=40)
    price_points = [
        (old, 0),
        (old - timedelta(days=1), 0),
    ]
    payload = {
        "currency": "JPY",
        "products": [
            {
                "csv": {
                    "AMAZON": _compact_series(price_points),
                },
                "title": "Old Product",
                "imagesCSV": "",
            }
        ],
    }
    session = RecordingSession([_make_response(payload)])
    client = _build_client(session)

    result = client.get_price_snapshot(SimpleNamespace(asin="ASIN", barcode=None))
    assert result.data is not None
    assert result.flags.degraded is True
    assert result.flags.reason in {"keepa_insufficient_data", "keepa_rank_insufficient"}
    assert result.data.current_price == Decimal("0")
