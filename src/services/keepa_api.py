from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional

import pandas as pd
import requests

from src.common.models import KeepaPriceSnapshot, ProductQuery


logger = logging.getLogger(__name__)


class KeepaAPIError(RuntimeError):
    """Raised when the Keepa API returns an error response."""


class KeepaAPIClient:
    BASE_URL = "https://api.keepa.com/product"

    def __init__(self, api_key: str, session: Optional[requests.Session] = None) -> None:
        self._api_key = api_key
        self._session = session or requests.Session()

    def get_price_snapshot(self, query: ProductQuery) -> KeepaPriceSnapshot:
        params = self._build_params(query)
        logger.debug("Calling Keepa API with params=%s", params)
        try:
            response = self._session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise KeepaAPIError(f"Keepa API request failed: {exc}") from exc

        payload = response.json()
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

        return KeepaPriceSnapshot(
            current_price=current_price or Decimal("0"),
            average_price_30d=average or Decimal("0"),
            lowest_price_30d=lowest or Decimal("0"),
            highest_price_30d=highest or Decimal("0"),
            sales_rank=stats.get("salesRankDrops30") or stats.get("current_SALES"),
            currency=payload.get("currency", "JPY"),
            title=product.get("title"),
            image_urls=image_urls,
        )

    def _build_params(self, query: ProductQuery) -> Dict[str, str]:
        params: Dict[str, str] = {
            "key": self._api_key,
            "domain": "5",  # 5 corresponds to Amazon.co.jp
            "stats": "90",  # request aggregated stats
            "offers": "20",
        }
        if query.asin:
            params["asin"] = query.asin
        elif query.barcode:
            params["code"] = query.barcode
        return params


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
