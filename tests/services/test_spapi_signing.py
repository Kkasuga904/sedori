from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
import requests

from src.common.config import RetrySettings, SPAPISettings
from src.common.rate_limit import CircuitBreaker, KeySemaphore, RequestBudget
from src.services.amazon_sp_api import AmazonSPAPIClient


class RecordingSession:
    def __init__(self, responses: list[requests.Response]):
        self._responses = list(responses)
        self.calls = 0
        self.last_kwargs: dict | None = None

    def request(self, method: str, url: str, **kwargs):
        idx = min(self.calls, len(self._responses) - 1)
        self.calls += 1
        self.last_kwargs = {"method": method, "url": url, **kwargs}
        return self._responses[idx]


@pytest.fixture
def spapi_components() -> tuple[SPAPISettings, RetrySettings, RequestBudget, KeySemaphore, CircuitBreaker]:
    settings = SPAPISettings(
        marketplace_id="TEST",
        region="us-west-2",
        lwa_client_id="dummy",
        lwa_client_secret="dummy",
        refresh_token="dummy",
        aws_access_key="AKIAEXAMPLE",
        aws_secret_key="SECRETKEYEXAMPLE",
        role_arn="dummy",
        default_currency="JPY",
    )
    retry = RetrySettings(max_attempts=2, base=0.01, max_sleep=0.01)
    budget = RequestBudget()
    semaphore = KeySemaphore(max_inflight=1)
    circuit = CircuitBreaker()
    return settings, retry, budget, semaphore, circuit


def _make_response(status_code: int, payload: dict | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(payload or {}).encode("utf-8")
    return response


def test_spapi_includes_sigv4_headers(spapi_components) -> None:
    settings, retry, budget, semaphore, circuit = spapi_components
    response = _make_response(200, {"payload": []})
    session = RecordingSession([response])
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
    assert session.last_kwargs is not None
    headers = session.last_kwargs["headers"]
    assert "Authorization" in headers
    assert headers["Authorization"].startswith("AWS4-HMAC-SHA256")
    assert headers["x-amz-access-token"] == "token"
    assert headers["host"] == "sellingpartnerapi-fe.amazon.com"


def test_spapi_signs_post_body(spapi_components) -> None:
    settings, retry, budget, semaphore, circuit = spapi_components
    response = _make_response(200, {"payload": {"FeesEstimatorResult": {"FeesEstimate": {"TotalFees": []}}}})
    session = RecordingSession([response])
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

    client.get_fees_estimate("ASIN", Decimal("1000"))
    assert session.last_kwargs is not None
    headers = session.last_kwargs["headers"]
    assert headers["Authorization"].startswith("AWS4-HMAC-SHA256")
    assert headers["Content-Type"] == "application/json"
    assert isinstance(session.last_kwargs["data"], (bytes, bytearray))
