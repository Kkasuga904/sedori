from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from threading import Lock
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from cachetools import TTLCache
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from src.common.config import CacheSettings, KeepaSettings, RetrySettings
from src.common.models import KeepaPriceSnapshot, ProductQuery, ServiceFlags, ServiceResult
from src.common.rate_limit import BudgetExceeded, CircuitBreaker, CircuitOpen, KeySemaphore, RequestBudget

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = (2.0, 5.0)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
KEEPA_EPOCH = datetime(2011, 1, 1, tzinfo=timezone.utc)
PRICE_SERIES_PRIORITY: Tuple[Tuple[str, ...], ...] = (
    ("AMAZON", "0"),
    ("NEW", "1", "NEW_FBA", "NEW_SHIPPING"),
    ("BUY_BOX_SHIPPING", "BUY_BOX", "16"),
)
RANK_SERIES_KEYS: Tuple[str, ...] = ("SALES", "SALES_RANK", "RANK", "3")
MIN_WINDOW_POINTS = 2


class KeepaAPIError(RuntimeError):
    """Raised when the Keepa API returns an unrecoverable error."""


class RetryableKeepaError(RuntimeError):
    """Internal marker for retry-eligible failures."""


@dataclass(frozen=True)
class _PriceSummary:
    current: Decimal
    median: Decimal
    p10: Decimal
    p90: Decimal
    insufficient: bool


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
        csv_map = _normalize_csv_map(product.get("csv"))
        price_summary, price_flags = _build_price_summary(csv_map)
        rank_value, rank_insufficient = _extract_rank(csv_map)

        if price_summary is None:
            price_summary = _PriceSummary(
                current=Decimal("0"),
                median=Decimal("0"),
                p10=Decimal("0"),
                p90=Decimal("0"),
                insufficient=True,
            )

        snapshot = KeepaPriceSnapshot(
            current_price=price_summary.current,
            average_price_30d=price_summary.median,
            lowest_price_30d=price_summary.p10,
            highest_price_30d=price_summary.p90,
            sales_rank=rank_value,
            currency=payload.get("currency", "JPY"),
            title=product.get("title"),
            image_urls=_extract_image_urls(product),
        )

        flags = _merge_flags(outcome.flags, price_flags)
        if rank_insufficient:
            flags = _merge_flags(flags, ServiceFlags(degraded=True, reason="keepa_rank_insufficient"))

        with self._cache_lock:
            self._cache[cache_key] = snapshot

        return ServiceResult(data=snapshot, flags=flags)

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


def _normalize_csv_map(csv_raw: object) -> Dict[str, Sequence[int]]:
    if isinstance(csv_raw, dict):
        normalized: Dict[str, Sequence[int]] = {}
        for key, value in csv_raw.items():
            if isinstance(value, Sequence):
                normalized[str(key).upper()] = value
        return normalized
    if isinstance(csv_raw, list):
        return {"DEFAULT": csv_raw}
    return {}


def _build_price_summary(csv_map: Dict[str, Sequence[int]]) -> tuple[Optional[_PriceSummary], ServiceFlags]:
    series = _select_series(csv_map, PRICE_SERIES_PRIORITY)
    if not series:
        return None, ServiceFlags(degraded=True, reason="keepa_insufficient_data")

    points = _decode_compact_series(series)
    if not points:
        return None, ServiceFlags(degraded=True, reason="keepa_insufficient_data")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    window_points = [(ts, value) for ts, value in points if ts >= cutoff and value > 0]
    insufficient_window = len(window_points) < MIN_WINDOW_POINTS
    if not window_points:
        window_points = [(ts, value) for ts, value in points if value > 0]

    if not window_points:
        return None, ServiceFlags(degraded=True, reason="keepa_insufficient_data")

    prices = [_quantize_price(value) for _, value in window_points]
    prices_sorted = sorted(prices)
    median = _median_decimal(prices_sorted)
    p10 = _percentile_decimal(prices_sorted, 0.10)
    p90 = _percentile_decimal(prices_sorted, 0.90)
    latest_price = _latest_price(points) or prices_sorted[-1]

    summary = _PriceSummary(
        current=latest_price,
        median=median,
        p10=p10,
        p90=p90,
        insufficient=insufficient_window,
    )
    flags = ServiceFlags(degraded=summary.insufficient, reason="keepa_insufficient_data" if summary.insufficient else None)
    return summary, flags


def _extract_rank(csv_map: Dict[str, Sequence[int]]) -> tuple[Optional[int], bool]:
    series = _select_series(csv_map, (RANK_SERIES_KEYS,))
    if not series:
        return None, True

    points = _decode_compact_series(series)
    if not points:
        return None, True

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    window_ranks = [value for ts, value in points if ts >= cutoff and value > 0]
    latest_rank = _latest_positive(points)
    if latest_rank is None and window_ranks:
        latest_rank = _median_int(window_ranks)
    insufficient = not window_ranks
    return latest_rank, insufficient


def _select_series(csv_map: Dict[str, Sequence[int]], priority: Iterable[Tuple[str, ...]]) -> Optional[Sequence[int]]:
    for aliases in priority:
        for alias in aliases:
            key = alias.upper()
            if key in csv_map and csv_map[key]:
                return csv_map[key]
    return None


def _decode_compact_series(series: Sequence[int]) -> List[Tuple[datetime, int]]:
    if not isinstance(series, Sequence):
        return []

    results: List[Tuple[datetime, int]] = []
    absolute_minutes: Optional[int] = None
    for index in range(0, len(series), 2):
        try:
            minutes_component = int(series[index])
        except (TypeError, ValueError):
            continue
        if absolute_minutes is None:
            absolute_minutes = minutes_component
        else:
            absolute_minutes += minutes_component
        if index + 1 >= len(series):
            break
        try:
            value_component = int(series[index + 1])
        except (TypeError, ValueError):
            continue
        timestamp = KEEPA_EPOCH + timedelta(minutes=absolute_minutes)
        results.append((timestamp, value_component))
    return results


def _quantize_price(value: int) -> Decimal:
    if value <= 0:
        return Decimal("0")
    amount = Decimal(value) / Decimal("100")
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _median_decimal(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    sorted_values = sorted(values)
    count = len(sorted_values)
    mid = count // 2
    if count % 2 == 1:
        return sorted_values[mid]
    avg = (sorted_values[mid - 1] + sorted_values[mid]) / Decimal("2")
    return avg.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _percentile_decimal(values: Sequence[Decimal], percentile: float) -> Decimal:
    if not values:
        return Decimal("0")
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * percentile
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    fraction = Decimal(str(rank - lower_index))
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    interpolated = lower_value + (upper_value - lower_value) * fraction
    return interpolated.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _latest_price(points: Sequence[Tuple[datetime, int]]) -> Optional[Decimal]:
    for _, value in sorted(points, key=lambda item: item[0], reverse=True):
        if value > 0:
            return _quantize_price(value)
    return None


def _latest_positive(points: Sequence[Tuple[datetime, int]]) -> Optional[int]:
    for _, value in sorted(points, key=lambda item: item[0], reverse=True):
        if value > 0:
            return int(value)
    return None


def _median_int(values: Sequence[int]) -> int:
    if not values:
        return 0
    sorted_values = sorted(values)
    count = len(sorted_values)
    mid = count // 2
    if count % 2 == 1:
        return sorted_values[mid]
    return int(round((sorted_values[mid - 1] + sorted_values[mid]) / 2))


def _merge_flags(primary: ServiceFlags, secondary: ServiceFlags) -> ServiceFlags:
    return ServiceFlags(
        degraded=primary.degraded or secondary.degraded,
        cached=primary.cached or secondary.cached,
        circuit_open=primary.circuit_open or secondary.circuit_open,
        reason=secondary.reason or primary.reason,
    )


def _extract_image_urls(product: Dict[str, object]) -> list[str]:
    images_csv = product.get("imagesCSV") or ""
    if not isinstance(images_csv, str):
        return []
    urls: List[str] = []
    for token in images_csv.split(","):
        token = token.strip()
        if not token:
            continue
        if token.startswith("http"):
            urls.append(token)
        else:
            urls.append(f"https://images-na.ssl-images-amazon.com/images/I/{token}.jpg")
    return urls
