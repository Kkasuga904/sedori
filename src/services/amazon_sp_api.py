from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from src.common.config import RetrySettings, SPAPISettings
from src.common.models import CompetitivePrice, FeeBreakdown, ProductQuery, ServiceFlags, ServiceResult
from src.common.rate_limit import BudgetExceeded, CircuitBreaker, CircuitOpen, KeySemaphore, RequestBudget

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = (2.0, 5.0)


class AmazonSPAPIError(RuntimeError):
    """Raised when the Amazon SP-API returns an unrecoverable error."""


class RetryableRequestError(RuntimeError):
    """Internal marker for retry-eligible failures."""


class AmazonSPAPIClient:
    def __init__(
        self,
        settings: SPAPISettings,
        retry: RetrySettings,
        access_token_provider,
        budget: RequestBudget,
        budget_limit: int,
        semaphore: KeySemaphore,
        circuit_breaker: CircuitBreaker,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._settings = settings
        self._retry = retry
        self._session = session or requests.Session()
        self._access_token_provider = access_token_provider
        self._budget = budget
        self._budget_limit = budget_limit
        self._semaphore = semaphore
        self._circuit_breaker = circuit_breaker

    def get_competitive_pricing(self, query: ProductQuery) -> ServiceResult[List[CompetitivePrice]]:
        params = self._build_pricing_params(query)
        outcome = self._request(
            "GET",
            "/products/pricing/v0/competitivePrice",
            request_key=self._budget_key(),
            params=params,
        )
        if outcome.data is None:
            return ServiceResult(data=[], flags=outcome.flags)

        payload = outcome.data.json()
        logger.debug("Competitive pricing payload: %s", payload)
        offers: List[CompetitivePrice] = []
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
        return ServiceResult(data=offers, flags=outcome.flags)

    def get_fees_estimate(self, asin: str, price: Decimal) -> ServiceResult[FeeBreakdown]:
        currency = self._settings.default_currency
        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": self._settings.marketplace_id,
                "Identifier": asin,
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": currency, "Amount": str(price)}
                },
                "IdentifierValue": asin,
                "OptionalFulfillmentPrograms": ["FBA"],
            }
        }
        outcome = self._request(
            "POST",
            "/products/fees/v0/listings/fees",
            request_key=self._budget_key(),
            json=body,
        )
        if outcome.data is None:
            return ServiceResult(data=None, flags=outcome.flags)

        payload = outcome.data.json()
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

        return ServiceResult(
            data=FeeBreakdown(
                referral_fee=breakdown.get("ReferralFee", Decimal("0")),
                closing_fee=breakdown.get("VariableClosingFee", Decimal("0")),
                fba_fee=breakdown.get("FBAPerUnitFulfillmentFee", Decimal("0")),
                inbound_shipping=breakdown.get("FBAShipmentFee", Decimal("0")),
                packaging_materials=Decimal("0"),
                storage_fee=Decimal("0"),
                taxes=breakdown.get("Tax", Decimal("0")),
                fx_spread=Decimal("0"),
                returns_cost=Decimal("0"),
                other_costs=Decimal("0"),
            ),
            flags=outcome.flags,
        )

    def _build_pricing_params(self, query: ProductQuery) -> Dict[str, str]:
        params = {"MarketplaceId": self._settings.marketplace_id}
        if query.asin:
            params["Asins"] = query.asin
        if query.barcode:
            params["Skus"] = query.barcode
        return params

    def _budget_key(self) -> str:
        return f"spapi:{self._settings.marketplace_id}"

    def _request(self, method: str, path: str, request_key: str, **kwargs) -> ServiceResult[requests.Response]:
        try:
            self._circuit_breaker.allow()
        except CircuitOpen as exc:
            logger.warning("Circuit breaker open for %s: %s", request_key, exc)
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, circuit_open=True, reason="circuit_open"))

        headers = kwargs.pop("headers", {})
        headers["Accept"] = "application/json"
        headers["x-amz-access-token"] = self._access_token_provider()

        retryer = Retrying(
            reraise=True,
            stop=stop_after_attempt(self._retry.max_attempts),
            wait=wait_exponential_jitter(initial=self._retry.base, max=self._retry.max_sleep),
            retry=retry_if_exception_type(RetryableRequestError),
        )

        try:
            response: requests.Response = retryer(self._send_once, method, path, request_key, headers, kwargs)
        except BudgetExceeded as exc:
            logger.warning("SP-API budget exhausted for %s", request_key)
            return ServiceResult(
                data=None,
                flags=ServiceFlags(degraded=True, reason="budget_exceeded"),
            )
        except RetryableRequestError as exc:
            logger.error("SP-API retries exhausted for %s: %s", request_key, exc)
            self._circuit_breaker.record_failure()
            return ServiceResult(
                data=None,
                flags=ServiceFlags(degraded=True, reason="retry_exhausted"),
            )
        except AmazonSPAPIError as exc:
            self._circuit_breaker.record_failure()
            raise
        except Exception as exc:  # pragma: no cover - defensive safeguard
            self._circuit_breaker.record_failure()
            raise AmazonSPAPIError(f"Unexpected SP-API failure: {exc}") from exc

        self._circuit_breaker.record_success()
        return ServiceResult(data=response, flags=ServiceFlags())

    def _send_once(
        self,
        method: str,
        path: str,
        request_key: str,
        headers: Dict[str, str],
        kwargs: Dict[str, object],
    ) -> requests.Response:
        self._budget.consume(request_key, self._budget_limit)
        with self._semaphore.acquire(request_key):
            try:
                response = self._session.request(
                    method=method,
                    url=f"https://sellingpartnerapi-fe.amazon.com{path}",
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    **kwargs,
                )
            except requests.Timeout as exc:
                raise RetryableRequestError(f"timeout: {exc}") from exc
            except requests.ConnectionError as exc:
                raise RetryableRequestError(f"connection error: {exc}") from exc
            except requests.RequestException as exc:
                raise AmazonSPAPIError(f"Amazon SP-API request failed: {exc}") from exc

        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableRequestError(
                f"retryable status {response.status_code} for path {path}"
            )
        if response.status_code >= 400:
            raise AmazonSPAPIError(
                f"Amazon SP-API request failed (status={response.status_code}, detail={response.text})"
            )
        return response




