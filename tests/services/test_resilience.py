from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests

from src.common.config import CacheSettings, KeepaSettings, RetrySettings, SPAPISettings
from src.common.rate_limit import CircuitBreaker, KeySemaphore, RequestBudget
from src.services.amazon_sp_api import AmazonSPAPIClient
from src.services.keepa_api import KeepaAPIClient


class StubResponse(requests.Response):
    def __init__(self, status_code: int, payload: dict | None = None):
        super().__init__()
        self.status_code = status_code
        self._content = json.dumps(payload or {}).encode("utf-8")


class SequenceSession:
    def __init__(self, responses, exceptions=None):
        self._responses = list(responses)
        self._exceptions = list(exceptions or [])
        self.calls = 0

    def request(self, *args, **kwargs):
        if self._exceptions and self.calls < len(self._exceptions):
            exc = self._exceptions[self.calls]
            self.calls += 1
            if exc is not None:
                raise exc
        idx = min(self.calls, len(self._responses) - 1)
        response = self._responses[idx]
        self.calls += 1
        return response

    def get(self, *args, **kwargs):
        return self.request(*args, **kwargs)


@pytest.fixture
def spapi_client():
    settings = SPAPISettings(
        marketplace_id="TEST",
        region="JP",
        lwa_client_id="dummy",
        lwa_client_secret="dummy",
        refresh_token="dummy",
        aws_access_key="dummy",
        aws_secret_key="dummy",
        role_arn="dummy",
        default_currency="JPY",
    )
    retry = RetrySettings(max_attempts=2, base=0.01, max_sleep=0.01)
    budget = RequestBudget()
    semaphore = KeySemaphore(max_inflight=1)
    circuit = CircuitBreaker()
    return settings, retry, budget, semaphore, circuit


def test_spapi_retries_and_degrades_on_rate_limit(spapi_client) -> None:
    settings, retry, budget, semaphore, circuit = spapi_client
    session = SequenceSession([StubResponse(429)])
    client = AmazonSPAPIClient(
        settings,
        retry,
        access_token_provider=lambda: "token",
        budget=budget,
        budget_limit=3,
        semaphore=semaphore,
        circuit_breaker=circuit,
        session=session,
    )
    result = client.get_competitive_pricing(SimpleNamespace(asin="ASIN", barcode=None))
    assert result.data == []
    assert result.flags.degraded is True
    assert result.flags.reason == "retry_exhausted"
    assert session.calls == retry.max_attempts


def test_spapi_recovers_after_timeout(spapi_client) -> None:
    settings, retry, budget, semaphore, circuit = spapi_client
    session = SequenceSession(
        responses=[StubResponse(200, payload={"payload": []})],
        exceptions=[requests.Timeout(), None],
    )
    client = AmazonSPAPIClient(
        settings,
        retry,
        access_token_provider=lambda: "token",
        budget=budget,
        budget_limit=5,
        semaphore=semaphore,
        circuit_breaker=circuit,
        session=session,
    )
    result = client.get_competitive_pricing(SimpleNamespace(asin="ASIN", barcode=None))
    assert result.flags.degraded is False
    assert session.calls >= 2


def test_keepa_degrades_on_server_error() -> None:
    settings = KeepaSettings(api_key="dummy", domain=5)
    retry = RetrySettings(max_attempts=2, base=0.01, max_sleep=0.01)
    cache = CacheSettings(ttl_seconds=1, cleanup_interval=1)
    budget = RequestBudget()
    semaphore = KeySemaphore(max_inflight=1)
    circuit = CircuitBreaker()
    session = SequenceSession([StubResponse(500)])
    client = KeepaAPIClient(
        settings,
        retry,
        cache,
        budget=budget,
        budget_limit=3,
        semaphore=semaphore,
        circuit_breaker=circuit,
        session=session,
    )
    result = client.get_price_snapshot(SimpleNamespace(asin="ASIN", barcode=None))
    assert result.data is None
    assert result.flags.degraded is True
    assert result.flags.reason == "retry_exhausted"



