from __future__ import annotations

import logging
import io
from typing import Any, Dict, List

import pytest

from src.common.config import LineSettings, RetrySettings, SlackSettings
from src.common.logging import configure_logging
from src.services.notifier import LINE_NOTIFY_URL, Notifier


class DummyResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.closed = False

    def close(self) -> None:
        self.closed = True


def build_notifier(line_enabled: bool = True, line_token: str | None = "DUMMY") -> Notifier:
    slack_settings = SlackSettings(enabled=False)
    line_settings = LineSettings(enabled=line_enabled, token=line_token)
    retry_settings = RetrySettings(max_attempts=3, base=0.001, max_sleep=0.002)
    return Notifier(slack_settings, line_settings, retry_settings, timeout_seconds=0.01)


def test_line_notify_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[Dict[str, Any]] = []

    def fake_post(url: str, *, headers: Dict[str, str], data: Dict[str, str], timeout: float) -> DummyResponse:
        response = DummyResponse(200)
        calls.append({"url": url, "headers": headers, "data": data, "timeout": timeout, "response": response})
        return response

    monkeypatch.setattr("src.services.notifier.httpx.post", fake_post)

    notifier = build_notifier()
    notifier.post_line("hello line")

    assert len(calls) == 1
    call = calls[0]
    assert call["url"] == LINE_NOTIFY_URL
    assert call["headers"]["Authorization"] == "Bearer DUMMY"
    assert call["data"] == {"message": "hello line"}
    assert isinstance(call["timeout"], float)
    assert call["response"].closed is True


def test_line_notify_retries_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [DummyResponse(429), DummyResponse(200)]
    calls: List[Dict[str, Any]] = []

    def fake_post(url: str, *, headers: Dict[str, str], data: Dict[str, str], timeout: float) -> DummyResponse:
        response = responses.pop(0)
        calls.append({"response": response})
        return response

    monkeypatch.setattr("src.services.notifier.httpx.post", fake_post)

    notifier = build_notifier()
    notifier.post_line("retry please")

    assert len(calls) == 2
    assert all(call["response"].closed for call in calls)


def test_logging_redacts_line_token() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", json_logs=False, secrets={"line_token": "SECRET123"}, stream=stream)
    logger = logging.getLogger("redaction-test")
    logger.info("Sending token SECRET123 to log")
    output = stream.getvalue()
    assert "***REDACTED***" in output
    assert "SECRET123" not in output
