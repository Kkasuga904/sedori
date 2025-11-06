from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Lock
from typing import Dict, Optional

import pandas as pd
import requests
from cachetools import TTLCache
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from src.common.config import CacheSettings, KeepaSettings, RetrySettings
from src.common.models import KeepaPriceSnapshot, ProductQuery, ServiceFlags, ServiceResult
from src.common.rate_limit import BudgetExceeded, CircuitBreaker, CircuitOpen, KeySemaphore, RequestBudget

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = (2.0, 5.0)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class KeepaAPIError(RuntimeError):
    """Raised when the Keepa API returns an unrecoverable error."""


class RetryableKeepaError(RuntimeError):
    """Internal marker for retry-eligible failures."""


class KeepaAPIClient:
    BASE_URL = "https://api.keepa.com/product"

    def __init__(
        self,
        settings: KeepaSettings,
        retry: RetrySettings,
        cache_settings: CacheSettings,
        budget: RequestBudget,
        budget_limit: int,
        semaphore: KeySemaphore,
        circuit_breaker: CircuitBreaker,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._settings = settings
        self._retry = retry
        self._session = session or requests.Session()
        self._budget = budget
        self._budget_limit = budget_limit
        self._semaphore = semaphore
        self._circuit_breaker = circuit_breaker
        self._cache = TTLCache(maxsize=512, ttl=cache_settings.ttl_seconds)
        self._cache_lock = Lock()

    def get_price_snapshot(self, query: ProductQuery) -> ServiceResult[KeepaPriceSnapshot]:
        cache_key = self._cache_key(query)
        with self._cache_lock:
            cached = self._cache.get(cache_key)
        if cached:
            return ServiceResult(data=cached, flags=ServiceFlags(cached=True))

        params = self._build_params(query)
        outcome = self._request(params, request_key=self._budget_key())
        if outcome.data is None:
            return ServiceResult(data=None, flags=outcome.flags)

        payload = outcome.data
        logger.debug("Keepa payload: %s", payload)

        if payload.get("error"):
            raise KeepaAPIError(f"Keepa API returned error: {payload['error']}")

        products = payload.get("products") or []
        if not products:
            raise KeepaAPIError("Keepa API response did not include product data")

        product = products[0]
        stats = product.get("stats", {})

        price_history = product.get("csv", [])
        current_price, lowest, highest, average = _compute_price_stats(price_history)

        image_urls = _extract_image_urls(product)

        snapshot = KeepaPriceSnapshot(
            current_price=current_price or Decimal("0"),
            average_price_30d=average or Decimal("0"),
            lowest_price_30d=lowest or Decimal("0"),
            highest_price_30d=highest or Decimal("0"),
            sales_rank=stats.get("salesRankDrops30") or stats.get("current_SALES"),
            currency=payload.get("currency", "JPY"),
            title=product.get("title"),
            image_urls=image_urls,
        )

        with self._cache_lock:
            self._cache[cache_key] = snapshot

        return ServiceResult(data=snapshot, flags=outcome.flags)

    def _budget_key(self) -> str:
        digest = hashlib.sha1(self._settings.api_key.get_secret_value().encode("utf-8")).hexdigest()[:6]
        return f"keepa:{self._settings.domain}:{digest}"

    def _cache_key(self, query: ProductQuery) -> str:
        identifier = query.asin or query.barcode
        if not identifier:
            raise ValueError("Keepa query requires asin or barcode")
        return f"{identifier}:{self._settings.domain}"

    def _build_params(self, query: ProductQuery) -> Dict[str, str]:
        params: Dict[str, str] = {
            "key": self._settings.api_key.get_secret_value(),
            "domain": str(self._settings.domain),
            "stats": "90",
            "offers": "20",
        }
        if query.asin:
            params["asin"] = query.asin
        elif query.barcode:
            params["code"] = query.barcode
        return params

    def _request(self, params: Dict[str, str], request_key: str) -> ServiceResult[Dict[str, object]]:
        try:
            self._circuit_breaker.allow()
        except CircuitOpen as exc:
            logger.warning("Circuit breaker open for %s: %s", request_key, exc)
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, circuit_open=True, reason="circuit_open"))

        retryer = Retrying(
            reraise=True,
            stop=stop_after_attempt(self._retry.max_attempts),
            wait=wait_exponential_jitter(initial=self._retry.base, max=self._retry.max_sleep),
            retry=retry_if_exception_type(RetryableKeepaError),
        )

        try:
            payload: Dict[str, object] = retryer(self._send_once, params, request_key)
        except BudgetExceeded:
            logger.warning("Keepa budget exhausted for %s", request_key)
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, reason="budget_exceeded"))
        except RetryableKeepaError as exc:
            logger.error("Keepa retries exhausted for %s: %s", request_key, exc)
            self._circuit_breaker.record_failure()
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, reason="retry_exhausted"))
        except KeepaAPIError:
            self._circuit_breaker.record_failure()
            raise
        except Exception as exc:  # pragma: no cover - defensive safeguard
            self._circuit_breaker.record_failure()
            raise KeepaAPIError(f"Unexpected Keepa failure: {exc}") from exc

        self._circuit_breaker.record_success()
        return ServiceResult(data=payload, flags=ServiceFlags())

    def _send_once(self, params: Dict[str, str], request_key: str) -> Dict[str, object]:
        self._budget.consume(request_key, self._budget_limit)
        with self._semaphore.acquire(request_key):
            try:
                response = self._session.get(self.BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
                status = response.status_code
            except requests.Timeout as exc:
                raise RetryableKeepaError(f"timeout: {exc}") from exc
            except requests.ConnectionError as exc:
                raise RetryableKeepaError(f"connection error: {exc}") from exc
            except requests.RequestException as exc:
                raise KeepaAPIError(f"Keepa API request failed: {exc}") from exc

        if status in RETRYABLE_STATUS_CODES:
            raise RetryableKeepaError(f"retryable status {status}")
        if status >= 400:
            raise KeepaAPIError(f"Keepa API request failed (status={status}, detail={response.text})")

        try:
            payload: Dict[str, object] = response.json()
        except ValueError as exc:
            raise KeepaAPIError(f"Invalid JSON from Keepa: {exc}") from exc
        return payload


def _compute_price_stats(
    price_history: Optional[list],
) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Keepa returns CSV-encoded price history; convert to statistics using pandas.
    """

    if not price_history:
        return None, None, None, None

    records = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    for datapoint in price_history:
        try:
            timestamp_minutes, price_cents = datapoint
        except (TypeError, ValueError):
            continue
        timestamp = datetime.fromtimestamp(timestamp_minutes * 60, tz=timezone.utc)
        if timestamp < cutoff:
            continue
        if price_cents < 0:
            continue
        price_value = Decimal(price_cents) / Decimal("100")
        records.append({"timestamp": timestamp, "price": price_value})

    if not records:
        return None, None, None, None

    frame = pd.DataFrame(records)
    current_price = frame.sort_values("timestamp", ascending=False).iloc[0]["price"]
    lowest = frame["price"].min()
    highest = frame["price"].max()
    average = frame["price"].mean()

    return (
        Decimal(str(current_price)),
        Decimal(str(lowest)),
        Decimal(str(highest)),
        Decimal(str(average)),
    )


def _extract_image_urls(product: Dict[str, object]) -> list[str]:
    images_csv = product.get("imagesCSV") or ""
    if not isinstance(images_csv, str):
        return []
    urls = []
    for token in images_csv.split(","):
        token = token.strip()
        if not token:
            continue
        if token.startswith("http"):
            urls.append(token)
        else:
            urls.append(f"https://images-na.ssl-images-amazon.com/images/I/{token}.jpg")
    return urls

