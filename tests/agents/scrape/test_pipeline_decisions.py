from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.agents.scrape.pipeline import ScrapeAgent
from src.common.config import Settings
from src.common.models import CompetitivePrice, FeeBreakdown, KeepaPriceSnapshot, ServiceFlags
from src.services.amazon_sp_api import AmazonSPAPIError
from src.services.keepa_api import KeepaAPIError


FIXTURES_DIR = Path("tests/fixtures/decisions")


class StubAmazonClient:
    def __init__(self, pricing_result, fees_result, pricing_exc=None, fees_exc=None):
        self._pricing_result = pricing_result
        self._fees_result = fees_result
        self._pricing_exc = pricing_exc
        self._fees_exc = fees_exc

    def get_competitive_pricing(self, query):
        if self._pricing_exc:
            raise self._pricing_exc
        return self._pricing_result

    def get_fees_estimate(self, identifier, price):
        if self._fees_exc:
            raise self._fees_exc
        return self._fees_result


class StubKeepaClient:
    def __init__(self, result=None, error: Exception | None = None):
        self._result = result
        self._error = error

    def get_price_snapshot(self, query):
        if self._error:
            raise self._error
        return self._result


def build_settings() -> Settings:
    return Settings.model_validate(
        {
            "api": {
                "spapi": {
                    "marketplace_id": "TEST",
                    "region": "JP",
                    "lwa_client_id": "dummy",
                    "lwa_client_secret": "dummy",
                    "refresh_token": "dummy",
                    "aws_access_key": "dummy",
                    "aws_secret_key": "dummy",
                    "role_arn": "dummy",
                    "default_currency": "JPY",
                },
                "keepa": {"api_key": "dummy", "domain": 5},
            },
            "notify": {
                "slack": {"enabled": False, "channel": None, "webhook": None, "token": None},
                "line": {"enabled": False, "token": None},
            },
            "thresholds": {"min_profit": "500", "min_roi": "0.15", "max_rank": 50000},
            "retry": {"max_attempts": 2, "base": 0.01, "max_sleep": 0.02},
            "cache": {"ttl_seconds": 1, "cleanup_interval": 1},
            "money": {
                "rounding": "0.01",
                "fx_spread_bp": 120,
                "return_rate": "0.04",
                "storage_fee_monthly": "50",
                "inbound_shipping": "120",
                "packaging_materials": "80",
            },
            "budget": {"spapi": 10, "keepa": 10},
            "observability": {"json_logs": False, "log_level": "INFO"},
            "cli": {"stagger_jitter_seconds": 0, "spapi_max_inflight": 1, "keepa_max_inflight": 1},
        }
    )


def load_golden(name: str) -> dict:
    with (FIXTURES_DIR / f"{name}.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def make_service_result(data, *, degraded=False, cached=False, circuit=False, reason=None):
    return SimpleNamespace(data=data, flags=ServiceFlags(degraded=degraded, cached=cached, circuit_open=circuit, reason=reason))


def test_pipeline_buy_decision(tmp_path: Path) -> None:
    agent = ScrapeAgent(settings=build_settings())

    snapshot = KeepaPriceSnapshot(
        current_price=Decimal("4500"),
        average_price_30d=Decimal("4200"),
        lowest_price_30d=Decimal("3800"),
        highest_price_30d=Decimal("4700"),
        sales_rank=3000,
        currency="JPY",
        title="テスト商品",
        image_urls=["https://example.com/img.jpg"],
    )
    offers = [
        CompetitivePrice(
            condition="New",
            seller_id="SELLER1",
            landed_price=Decimal("4400"),
            shipping=Decimal("0"),
            last_updated=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    ]
    base_fees = FeeBreakdown(
        referral_fee=Decimal("480"),
        closing_fee=Decimal("0"),
        fba_fee=Decimal("250"),
        inbound_shipping=Decimal("0"),
        packaging_materials=Decimal("0"),
        storage_fee=Decimal("0"),
        taxes=Decimal("30"),
        fx_spread=Decimal("0"),
        returns_cost=Decimal("0"),
        other_costs=Decimal("0"),
    )

    agent._keepa_client = StubKeepaClient(make_service_result(snapshot))  # type: ignore[attr-defined]
    agent._amazon_client = StubAmazonClient(  # type: ignore[attr-defined]
        pricing_result=make_service_result(offers),
        fees_result=make_service_result(base_fees),
    )

    result = agent.run(
        asin="TESTASIN",
        barcode=None,
        purchase_cost=Decimal("2400"),
        taxes=None,
        target_price=Decimal("4800"),
        notify_slack=False,
        dry_run=True,
        decision_path=None,
        request_id="test-buy",
    )

    assert result == load_golden("buy")


def test_pipeline_no_buy_due_to_rank() -> None:
    agent = ScrapeAgent(settings=build_settings())
    snapshot = KeepaPriceSnapshot(
        current_price=Decimal("3000"),
        average_price_30d=Decimal("3100"),
        lowest_price_30d=Decimal("2800"),
        highest_price_30d=Decimal("3300"),
        sales_rank=999999,
        currency="JPY",
        title="Slow Seller",
        image_urls=[],
    )
    offers: list[CompetitivePrice] = []
    base_fees = FeeBreakdown(referral_fee=Decimal("200"), fba_fee=Decimal("150"), taxes=Decimal("20"))

    agent._keepa_client = StubKeepaClient(make_service_result(snapshot))  # type: ignore[attr-defined]
    agent._amazon_client = StubAmazonClient(  # type: ignore[attr-defined]
        pricing_result=make_service_result(offers),
        fees_result=make_service_result(base_fees),
    )

    result = agent.run(
        asin="SLOWASIN",
        barcode=None,
        purchase_cost=Decimal("2500"),
        taxes=None,
        target_price=Decimal("3200"),
        notify_slack=False,
        dry_run=True,
        decision_path=None,
        request_id="test-nobuy",
    )

    assert result == load_golden("no_buy")


def test_pipeline_degraded_on_failures() -> None:
    agent = ScrapeAgent(settings=build_settings())

    cached_snapshot = KeepaPriceSnapshot(
        current_price=Decimal("0"),
        average_price_30d=Decimal("0"),
        lowest_price_30d=Decimal("0"),
        highest_price_30d=Decimal("0"),
        sales_rank=None,
        currency="JPY",
        title=None,
        image_urls=[],
    )

    agent._keepa_client = StubKeepaClient(make_service_result(cached_snapshot, degraded=True, cached=True, reason="keepa_cache"))  # type: ignore[attr-defined]
    agent._amazon_client = StubAmazonClient(  # type: ignore[attr-defined]
        pricing_result=make_service_result([], degraded=True, reason="spapi_pricing_error"),
        fees_result=make_service_result(FeeBreakdown(), degraded=True, reason="spapi_fee_error"),
        pricing_exc=None,
        fees_exc=AmazonSPAPIError("rate limit"),
    )

    result = agent.run(
        asin="DEGRADED",
        barcode=None,
        purchase_cost=Decimal("1000"),
        taxes=None,
        target_price=Decimal("1500"),
        notify_slack=False,
        dry_run=True,
        decision_path=None,
        request_id="test-degraded",
    )

    assert result == load_golden("degraded")
