from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

import requests

from src.common.models import CompetitivePrice, FeeBreakdown, ProductQuery


logger = logging.getLogger(__name__)


class AmazonSPAPIError(RuntimeError):
    """Raised when the Amazon SP-API returns an error response."""


class AmazonSPAPIClient:
    """
    Lightweight Amazon Selling Partner API client focusing on pricing and fees.

    The implementation targets the ``getCompetitivePricing`` and
    ``getMyFeesEstimate`` endpoints. Authentication boilerplate is intentionally
    omitted and should be provided by the caller via a valid access token.
    """

    BASE_URL = "https://sellingpartnerapi-fe.amazon.com"

    def __init__(
        self,
        config: Dict[str, str],
        access_token_provider,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._access_token_provider = access_token_provider

    def get_competitive_pricing(self, query: ProductQuery) -> List[CompetitivePrice]:
        params = self._build_pricing_params(query)
        logger.debug("Requesting competitive pricing with params=%s", params)
        response = self._request(
            "GET",
            "/products/pricing/v0/competitivePrice",
            params=params,
        )
        payload = response.json()
        logger.debug("Competitive pricing payload: %s", payload)
        offers = []
        for product in payload.get("payload", []):
            for offer in product.get("competitivePricing", {}).get("competitivePrices", []):
                price = offer.get("price", {})
                amount = Decimal(str(price.get("LandedPrice", {}).get("Amount", "0")))
                shipping = Decimal(str(price.get("Shipping", {}).get("Amount", "0")))
                offers.append(
                    CompetitivePrice(
                        condition=offer.get("condition", "Unknown"),
                        seller_id=offer.get("sellerId", "Unknown"),
                        landed_price=amount,
                        shipping=shipping,
                        last_updated=datetime.now(timezone.utc),
                    )
                )
        return offers

    def get_fees_estimate(self, asin: str, price: Decimal) -> FeeBreakdown:
        currency = self._config.get("currency") or self._config.get("default_currency", "JPY")
        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": self._config["marketplace_id"],
                "Identifier": asin,
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": currency, "Amount": str(price)}
                },
                "IdentifierValue": asin,
                "OptionalFulfillmentPrograms": ["FBA"],
            }
        }
        logger.debug("Requesting fees estimate for %s with body=%s", asin, body)
        response = self._request(
            "POST",
            "/products/fees/v0/listings/fees",
            json=body,
        )
        payload = response.json()
        logger.debug("Fees estimate payload: %s", payload)

        fees_section = (
            payload.get("payload", {})
            .get("FeesEstimatorResult", {})
            .get("FeesEstimate", {})
        )
        total_fees = fees_section.get("TotalFees", []) or []
        breakdown: Dict[str, Decimal] = {}
        for fee in total_fees:
            try:
                fee_type = fee["FeeType"]
                amount = Decimal(str(fee["FeeAmount"]["Amount"]))
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("Skipping malformed fee entry %s (%s)", fee, exc)
                continue
            breakdown[fee_type] = amount

        return FeeBreakdown(
            referral_fee=breakdown.get("ReferralFee", Decimal("0")),
            closing_fee=breakdown.get("VariableClosingFee", Decimal("0")),
            fba_fee=breakdown.get("FBAPerUnitFulfillmentFee", Decimal("0")),
            shipping_fee=breakdown.get("FBAShipmentFee", Decimal("0")),
            taxes=breakdown.get("Tax", Decimal("0")),
        )

    def _build_pricing_params(self, query: ProductQuery) -> Dict[str, str]:
        params = {"MarketplaceId": self._config["marketplace_id"]}
        if query.asin:
            params["Asins"] = query.asin
        if query.barcode:
            params["Skus"] = query.barcode
        return params

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["Accept"] = "application/json"
        headers["x-amz-access-token"] = self._access_token_provider()
        try:
            response = self._session.request(
                method=method, url=f"{self.BASE_URL}{path}", timeout=30, headers=headers, **kwargs
            )
        except requests.RequestException as exc:
            raise AmazonSPAPIError(f"Amazon SP-API request failed: {exc}") from exc
        if response.status_code == 429:
            raise AmazonSPAPIError("Amazon SP-API rate limit exceeded")
        if response.status_code >= 400:
            raise AmazonSPAPIError(
                f"Amazon SP-API request failed "
                f"(status={response.status_code}, detail={response.text})"
            )
        return response
